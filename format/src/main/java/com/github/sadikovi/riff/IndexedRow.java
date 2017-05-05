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

package com.github.sadikovi.riff;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Implementation of [[GenericInternalRow]] that provides index fields access as well as data fields
 * access. Has a restriction of only Long.SIZE fields to maintain. Assumes that order of fields
 * written is index fields -> data fields.
 */
final class IndexedRow extends GenericInternalRow {
  public static final byte MAGIC1 = 67;
  public static final byte MAGIC2 = 73;

  // relative (! not absolute) fixed part byte offsets for index region and data region
  private final int[] offsets;
  // bit set of indexed fields, 1 if field at ordinal is indexed, 0 otherwise
  private final long indexed;
  // null bit set, 1 if field is null, 0 otherwise
  private long nulls;
  // byte buffer for index region
  private ByteBuffer indexBuffer;
  // byte buffer for data region
  private ByteBuffer dataBuffer;

  IndexedRow(long indexed, long nulls, int[] offsets) {
    if (offsets.length > Long.SIZE) {
      throw new IllegalArgumentException(
        "Too many fields " + offsets.length + ", should be <= " + Long.SIZE);
    }
    this.offsets = offsets;
    this.indexed = indexed;
    this.nulls = nulls;
    this.indexBuffer = null;
    this.dataBuffer = null;
  }

  /**
   * Set index region as array of bytes, array should already be a copy - wrapped byte buffer is
   * created on top of this array.
   */
  protected void setIndexRegion(byte[] bytes) {
    this.indexBuffer = ByteBuffer.wrap(bytes);
  }

  /**
   * Set data region as array of bytes, array should already be a copy - wrapped byte buffer is
   * created, similar to index region.
   */
  protected void setDataRegion(byte[] bytes) {
    this.dataBuffer = ByteBuffer.wrap(bytes);
  }

  /**
   * Get nulls bit set, for internal use only.
   */
  protected long getNulls() {
    return this.nulls;
  }

  /**
   * Get indexed bit set, for internal use only.
   */
  protected long getIndexed() {
    return this.indexed;
  }

  /**
   * Whether or not this row has index region set.
   * @return true if region is set, false otherwise
   */
  public boolean hasIndexRegion() {
    return this.indexBuffer != null;
  }

  /**
   * Whether or not this row has data region set.
   * @return true if region is set, false otherwise
   */
  public boolean hasDataRegion() {
    return this.dataBuffer != null;
  }

  @Override
  public int numFields() {
    return this.offsets.length;
  }

  @Override
  public IndexedRow copy() {
    int[] copyOffsets = new int[this.offsets.length];
    System.arraycopy(this.offsets, 0, copyOffsets, 0, this.offsets.length);
    IndexedRow row = new IndexedRow(this.indexed, this.nulls, copyOffsets);
    if (hasIndexRegion()) {
      // copy bytes manually, array() method returns reference to wrapped byte array
      byte[] arr = new byte[this.indexBuffer.capacity()];
      System.arraycopy(this.indexBuffer.array(), 0, arr, 0, arr.length);
      row.setIndexRegion(arr);
    }
    if (hasDataRegion()) {
      // copy bytes manually, array() method returns reference to wrapped byte array
      byte[] arr = new byte[this.dataBuffer.capacity()];
      System.arraycopy(this.dataBuffer.array(), 0, arr, 0, arr.length);
      row.setDataRegion(arr);
    }
    return row;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof IndexedRow)) return false;
    if (other == this) return true;
    IndexedRow row = (IndexedRow) other;
    // we do not compare underlying byte buffers
    return row.numFields() == this.numFields() && row.getNulls() == this.getNulls() &&
      row.getIndexed() == this.getIndexed();
  }

  @Override
  public int hashCode() {
    int result = 31 * numFields();
    result += (int) (this.nulls ^ (this.nulls >>> 32));
    result += (int) (this.indexed ^ (this.indexed >>> 32));
    return result;
  }

  @Override
  public boolean anyNull() {
    return this.nulls != 0;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return (this.nulls & 1L << ordinal) != 0;
  }

  /**
   * Whether or not field for ordinal is indexed.
   * @return true if field is indexed, false otherwise
   */
  public boolean isIndexed(int ordinal) {
    return (this.indexed & 1L << ordinal) != 0;
  }

  @Override
  public int getInt(int ordinal) {
    if (isIndexed(ordinal)) {
      return this.indexBuffer.getInt(this.offsets[ordinal]);
    } else {
      return this.dataBuffer.getInt(this.offsets[ordinal]);
    }
  }

  @Override
  public long getLong(int ordinal) {
    if (isIndexed(ordinal)) {
      return this.indexBuffer.getLong(this.offsets[ordinal]);
    } else {
      return this.dataBuffer.getLong(this.offsets[ordinal]);
    }
  }

  /** Extract UTF8String from byte buffer */
  private UTF8String getUTF8String(int ordinal, ByteBuffer buf) {
    // parse metadata, this should be in sync with converters: [offset + length]
    long metadata = buf.getLong(this.offsets[ordinal]);
    int offset = (int) (metadata >>> 32);
    int length = (int) (metadata & Integer.MAX_VALUE);
    byte[] bytes = new byte[length];
    int oldPos = buf.position();
    // need to reset buffer position, offset in get method is applied to buffer as well, so we
    // use 0 as array offset and shift position to offset
    buf.position(offset);
    buf.get(bytes, 0, length);
    buf.position(oldPos);
    return UTF8String.fromBytes(bytes);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (isIndexed(ordinal)) {
      return getUTF8String(ordinal, this.indexBuffer);
    } else {
      return getUTF8String(ordinal, this.dataBuffer);
    }
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("nulls=" + anyNull() + ", ");
    sb.append("fields=" + numFields() + ", ");
    if (hasIndexRegion()) {
      sb.append("index_region=" + Arrays.toString(this.indexBuffer.array()));
    } else {
      sb.append("index_region=null");
    }
    sb.append(", ");
    if (hasDataRegion()) {
      sb.append("data_region=" + Arrays.toString(this.dataBuffer.array()));
    } else {
      sb.append("data_region=null");
    }
    sb.append("]");
    return sb.toString();
  }
}
