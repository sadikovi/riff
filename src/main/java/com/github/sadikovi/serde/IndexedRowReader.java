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
import java.io.EOFException;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.serde.io.InStream;

/**
 * Reader for [[IndexedRow]] instances, created per stream and reused across rows.
 * Type description is used directly to map schema to each row.
 */
public class IndexedRowReader {
  private final TypeDescription desc;
  // buffer for primitive fields (used for conversion)
  private byte[] buf;
  // bit set to mark indexed fields
  private long indexed;
  // set of converters to use
  private final RowValueConverter[] converters;

  public IndexedRowReader(TypeDescription desc) {
    this.desc = desc;
    this.buf = new byte[8];
    // compute index fields in bit set
    this.indexed = 0L;
    for (TypeSpec spec : this.desc.indexFields()) {
      if (spec.isIndexed()) {
        this.indexed |= 1L << spec.position();
      }
    }
    // initialize converters, they are reused across rows
    this.converters = new RowValueConverter[this.desc.size()];
    TypeSpec[] arr = this.desc.fields();
    for (TypeSpec spec : arr) {
      this.converters[spec.position()] = Converters.sqlTypeToConverter(spec.dataType());
    }
  }

  /**
   * Read row from input stream. This should reflect write logic in `IndexedRowWriter`. We check
   * magic byte and buffer optional null bit set; then copy index region and data region into
   * indexed row.
   *
   * @param in input stream to read from
   * @return indexed row as InternalRow
   */
  public InternalRow readRow(InStream in) throws IOException {
    int magic = in.read();
    if (magic != IndexedRow.MAGIC1 && magic != IndexedRow.MAGIC2) {
      throw new AssertionError("Wrong magic number " + magic);
    }
    long nulls = (magic == IndexedRow.MAGIC1) ? 0L : in.readLong();
    // prepare row, compute row offsets
    IndexedRow row = new IndexedRow(this.indexed, nulls, rowOffsets(nulls));
    // read index region, note that if no bytes were written, we do not set index region at all
    int indexBytes = in.readInt();
    if (indexBytes > 0) {
      byte[] indexRegion = new byte[indexBytes];
      in.read(indexRegion, 0, indexBytes);
      row.setIndexRegion(indexRegion);
    }
    // read data region, similarly we do not initialize data region, if no bytes were written
    int dataBytes = in.readInt();
    if (dataBytes > 0) {
      byte[] dataRegion = new byte[dataBytes];
      in.read(dataRegion, 0, dataBytes);
      row.setDataRegion(dataRegion);
    }
    return row;
  }

  /** Compute relative row offsets for indexed row */
  private int[] rowOffsets(long nulls) {
    int[] offsets = new int[this.desc.size()];
    // update index fields
    relativeRowOffset(offsets, nulls, this.desc.indexFields());
    // update data fields
    relativeRowOffset(offsets, nulls, this.desc.dataFields());
    return offsets;
  }

  /**
   * Relative row offset is computed for absolute position of the type spec, but relative to the
   * region, e.g. if 4 fields exist: 2 (long, int) in index region and 2 (int, int) in data region,
   * we will write:
   * +---+---+---+---+
   * | 0 | 1 | 2 | 3 |
   * +---+---+---+---+
   * | 0 | 8 | 0 | 4 |
   * +---+---+---+---+
   * If value is null, then we write -1 as default value and next non-null position starts from
   * previous non-null + offset.
   */
  private void relativeRowOffset(int[] offsets, long nulls, TypeSpec[] fields) {
    int offset = 0;
    for (int i = 0; i < fields.length; i++) {
      if ((nulls & 1L << fields[i].position()) == 0) {
        // update offset
        offsets[fields[i].position()] = offset;
        offset += this.converters[fields[i].position()].byteOffset();
      } else {
        // set null value
        offsets[fields[i].position()] = -1;
      }
    }
  }
}
