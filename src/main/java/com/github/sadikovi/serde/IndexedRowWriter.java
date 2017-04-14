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

  public void writeRow(InternalRow row, OutputStream out) throws IOException {
    prepareWrite();
    bufferIndexRegion(row);
    bufferDataRegion(row);
    // write values according to specification
    long bitset = getNullSet(row);
    if (bitset == 0) {
      out.write(0x0);
    } else {
      out.write(0x1);
      writeLong(bitset, out);
    }
    // TODO: add check on overflow
    writeInt(this.indexFixedBuffer.bytesWritten() + this.indexVariableBuffer.bytesWritten(), out);
    out.write(this.indexFixedBuffer.array());
    out.write(this.indexVariableBuffer.array());
    // TODO: add check on overflow
    writeInt(this.dataFixedBuffer.bytesWritten() + this.dataVariableBuffer.bytesWritten(), out);
    out.write(this.dataFixedBuffer.array());
    out.write(this.dataVariableBuffer.array());
  }

  private void prepareWrite() {
    this.indexFixedBuffer.reset();
    this.indexVariableBuffer.reset();
    this.dataFixedBuffer.reset();
    this.dataVariableBuffer.reset();
  }

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

  private void writeInt(int value, OutputStream out) throws IOException {
    out.write((byte) (0xff & (value >> 24)));
    out.write((byte) (0xff & (value >> 16)));
    out.write((byte) (0xff & (value >>  8)));
    out.write((byte) (0xff & value));
  }

  private void assertWrittenBytes(int bytes, int expectedBytes) {
    if (bytes != expectedBytes) {
      throw new AssertionError("Written bytes " + bytes + " != " + expectedBytes + " bytes");
    }
  }

  private void bufferRegion(
      InternalRow row,
      TypeSpec[] fields,
      OutputBuffer fixedBuffer,
      OutputBuffer variableBuffer) throws IOException {
    int fixedOffset = 0;
    for (int i = 0; i < fields.length; i++) {
      // TODO: check if using null bit set directly is faster
      if (!row.isNullAt(fields[i].origSQLPos())) {
        fixedOffset += this.converters[fields[i].position()].offset();
      }
    }
    for (int i = 0; i < fields.length; i++) {
      if (!row.isNullAt(fields[i].origSQLPos())) {
        this.converters[fields[i].position()].writeDirect(
          row, fields[i].origSQLPos(), fixedBuffer, fixedOffset, variableBuffer);
      }
    }
    assertWrittenBytes(fixedBuffer.bytesWritten(), fixedOffset);
  }

  private void bufferIndexRegion(InternalRow row) throws IOException {
    bufferRegion(row, this.desc.indexFields(), this.indexFixedBuffer, this.indexVariableBuffer);

  }

  private void bufferDataRegion(InternalRow row) throws IOException {
    bufferRegion(row, this.desc.dataFields(), this.dataFixedBuffer, this.dataVariableBuffer);
  }
}
