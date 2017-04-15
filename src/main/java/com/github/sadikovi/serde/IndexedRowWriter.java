/*
 * Copyright (c) 2017 sadikovi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.sadikovi.serde;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Writer for [[IndexedRow]] instances, created per stream and reused across rows.
 */
public class IndexedRowWriter {
  // type description for writer
  private final TypeDescription desc;
  // reused buffers for fixed and variable parts (for both index and data regions)
  private OutputBuffer indexFixedBuffer;
  private OutputBuffer indexVariableBuffer;
  private OutputBuffer dataFixedBuffer;
  private OutputBuffer dataVariableBuffer;
  // set of converters to use
  private final RowValueConverter[] converters;

  public IndexedRowWriter(TypeDescription desc) {
    this.desc = desc;
    // reused output buffers
    this.indexFixedBuffer = new OutputBuffer();
    this.indexVariableBuffer = new OutputBuffer();
    this.dataFixedBuffer = new OutputBuffer();
    this.dataVariableBuffer = new OutputBuffer();
    // initialize converters, they are reused across rows
    this.converters = new RowValueConverter[this.desc.size()];
    TypeSpec[] arr = this.desc.fields();
    for (TypeSpec spec : arr) {
      this.converters[spec.position()] = Converters.sqlTypeToConverter(spec.dataType());
    }
  }

  /**
   * Write content of internal row into output stream. This follows specification of [[IndexedRow]]
   * +-----------------------+----------------------------------+
   * | magic as is_null byte | optional null bit set as 8 bytes |
   * +-----------------------+----------------------------------+
   * ...
   * +------------------------+--------------+-----------------------+-------------+
   * | length of index region | index region | length of data region | data region |
   * +------------------------+--------------+-----------------------+-------------+
   * Each region is written similar to Spark SQL UnsafeRow: write fixed part first, where each
   * primitive type is written directly, variable type is written according to row converter,
   * usuallly metadata; write variable part of bytes back to back.
   * If value is null, bit is set and value is skipped.
   *
   * @param row row to write
   * @param out output stream to write to
   */
  public void writeRow(InternalRow row, OutputStream out) throws IOException {
    prepareWrite();
    // collect null information
    long bitset = getNullSet(row);
    // bufffer index region, this fills fixed and variable buffers
    bufferIndexRegion(row);
    // bufffer data region, this fills fixed and variable buffers
    bufferDataRegion(row);
    // write values according to specification
    // we use magic numbers to check if row is written correctly and as indicators of nullability
    // for each row; nulls are only written if exist
    if (bitset == 0) {
      out.write(IndexedRow.MAGIC1);
    } else {
      out.write(IndexedRow.MAGIC2);
      writeLong(bitset, out);
    }
    // write index region
    checkOverflow(this.indexFixedBuffer.bytesWritten(), this.indexVariableBuffer.bytesWritten());
    writeInt(this.indexFixedBuffer.bytesWritten() + this.indexVariableBuffer.bytesWritten(), out);
    this.indexFixedBuffer.writeExternal(out);
    this.indexVariableBuffer.writeExternal(out);
    // write data region
    checkOverflow(this.dataFixedBuffer.bytesWritten(), this.dataVariableBuffer.bytesWritten());
    writeInt(this.dataFixedBuffer.bytesWritten() + this.dataVariableBuffer.bytesWritten(), out);
    this.dataFixedBuffer.writeExternal(out);
    this.dataVariableBuffer.writeExternal(out);
  }

  /** Check if two numbers result in int overflow */
  private void checkOverflow(int value1, int value2) {
    if (Integer.MAX_VALUE - value1 < value2) {
      throw new AssertionError("Overflow (" + value1 + " + " + value2 + ")");
    }
  }

  /**
   * Prepare for write.
   * Method resets all buffers (index + data) and tracking data structures. Called once per write.
   */
  private void prepareWrite() {
    this.indexFixedBuffer.reset();
    this.indexVariableBuffer.reset();
    this.dataFixedBuffer.reset();
    this.dataVariableBuffer.reset();
  }

  /**
   * Mark all null fields in row in null bit set. This assumes that internal row matches provided
   * struct type from type description, meaning it uses original SQL position to access values, not
   * type spec position.
   */
  private long getNullSet(InternalRow row) {
    long bitset = 0L;
    int i = 0;
    while (i < this.desc.fields().length) {
      if (row.isNullAt(this.desc.fields()[i].origSQLPos())) {
        bitset |= 1L << this.desc.fields()[i].position();
      }
      ++i;
    }
    return bitset;
  }

  /** Write long value into stream, added since output stream does not have this method */
  private void writeLong(long value, OutputStream out) throws IOException {
    out.write((byte) (0xff & (value >> 56)));
    out.write((byte) (0xff & (value >> 48)));
    out.write((byte) (0xff & (value >> 40)));
    out.write((byte) (0xff & (value >> 32)));
    out.write((byte) (0xff & (value >> 24)));
    out.write((byte) (0xff & (value >> 16)));
    out.write((byte) (0xff & (value >>  8)));
    out.write((byte) (0xff & value));
  }

  /** Write int value into stream, added since output stream does not have this method */
  private void writeInt(int value, OutputStream out) throws IOException {
    out.write((byte) (0xff & (value >> 24)));
    out.write((byte) (0xff & (value >> 16)));
    out.write((byte) (0xff & (value >>  8)));
    out.write((byte) (0xff & value));
  }

  /**
   * Buffer current region into fixed and variable buffers.
   * If field is not null we write fixed portion of value and variable bytes (if applicable). For
   * variable part we maintain offset (as total number of bytes written for fixed part) so each row
   * converter is supposed to write necessary metadata to access raw bytes in variable part.
   */
  private void bufferRegion(
      InternalRow row,
      TypeSpec[] fields,
      OutputBuffer fixedBuffer,
      OutputBuffer variableBuffer) throws IOException {
    int fixedOffset = 0;
    // compute total offset for fixed part
    for (int i = 0; i < fields.length; i++) {
      if (!row.isNullAt(fields[i].origSQLPos())) {
        fixedOffset += this.converters[fields[i].position()].byteOffset();
      }
    }
    // write value using row converters
    for (int i = 0; i < fields.length; i++) {
      if (!row.isNullAt(fields[i].origSQLPos())) {
        this.converters[fields[i].position()].writeDirect(
          row, fields[i].origSQLPos(), fixedBuffer, fixedOffset, variableBuffer);
      }
    }
    // assertion should hold for fixed part
    if (fixedBuffer.bytesWritten() != fixedOffset) {
      throw new AssertionError(
        "Written bytes " + fixedBuffer.bytesWritten() + " != " + fixedOffset + " bytes");
    }
  }

  /** Buffer index region */
  private void bufferIndexRegion(InternalRow row) throws IOException {
    bufferRegion(row, this.desc.indexFields(), this.indexFixedBuffer, this.indexVariableBuffer);

  }

  /** Buffer data region */
  private void bufferDataRegion(InternalRow row) throws IOException {
    bufferRegion(row, this.desc.dataFields(), this.dataFixedBuffer, this.dataVariableBuffer);
  }
}
