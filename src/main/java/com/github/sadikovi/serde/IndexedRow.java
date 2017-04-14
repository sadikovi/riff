package com.github.sadikovi.serde;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
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
  public InternalRow copy() {
    int[] copyOffsets = new int[this.offsets.length];
    System.arraycopy(this.offsets, 0, copyOffsets, 0, this.offsets.length);
    IndexedRow row = new IndexedRow(this.indexed, this.nulls, copyOffsets);
    // TODO: perform direct copy on byte buffer
    if (hasIndexRegion()) {
      row.setIndexRegion(this.indexBuffer.array());
    }
    // TODO: perform direct copy on byte buffer
    if (hasDataRegion()) {
      row.setDataRegion(this.dataBuffer.array());
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    return this.nulls != 0;
  }

  //////////////////////////////////////////////////////////////
  // Specialized getters
  //////////////////////////////////////////////////////////////

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
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("nulls=" + anyNull() + ", ");
    if (hasIndexRegion()) {
      sb.append("index_region=" + Arrays.toString(this.indexBuffer.array()) + ", ");
    } else {
      sb.append("index_region=null, ");
    }
    if (hasDataRegion()) {
      sb.append("data_region=" + Arrays.toString(this.dataBuffer.array()) + ", ");
    } else {
      sb.append("data_region=null, ");
    }
    sb.append("]");
    return sb.toString();
  }
}
