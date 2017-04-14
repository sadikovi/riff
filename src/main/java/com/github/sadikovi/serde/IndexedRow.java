package com.github.sadikovi.serde;

import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Implementation of [[GenericInternalRow]] that provides index fields access as well as data fields
 * access. Has a restriction of only Long.SIZE fields to maintain. Assumes that order of fields
 * written is index fields -> data fields.
 */
public final class IndexedRow extends GenericInternalRow {
  // relative (! not absolute) fixed part byte offsets for index region and data region
  private final int[] offsets;
  // bit set of indexed fields, 1 if field at ordinal is indexed, 0 otherwise
  private final long indexed;
  // null bit set, 1 if field is null, 0 otherwise
  private long bitset;
  // byte buffer for index region
  private ByteBuffer indexBuffer;
  // byte buffer for data region
  private ByteBuffer dataBuffer;

  IndexedRow(long indexed, int[] offsets) {
    assertNumFields(offsets.length, Long.SIZE);
    this.offsets = offsets;
    this.indexed = indexed;
    this.bitset = 0L;
    this.indexBuffer = null;
    this.dataBuffer = null;
  }

  private static void assertNumFields(int numFields, int limit) {
    if (numFields < 0 || numFields > limit) {
      throw new AssertionError(
        "Number of fields " + numFields + " >= 0 and " + numFields + " <= " + limit);
    }
  }

  protected void setNulls(long bitset) {
    this.bitset = bitset;
  }

  protected void setIndexRegion(byte[] bytes, int offset, int length) {
    this.indexBuffer = ByteBuffer.wrap(bytes, offset, length);
  }

  protected void setDataRegion(byte[] bytes, int offset, int length) {
    this.dataBuffer = ByteBuffer.wrap(bytes, offset, length);
  }

  public boolean hasIndexRegion() {
    return this.indexBuffer != null;
  }

  public boolean hasDataRegion() {
    return this.dataBuffer != null;
  }

  public byte[] indexRegion() {
    return this.indexBuffer.array();
  }

  public byte[] dataRegion() {
    return this.dataBuffer.array();
  }

  public int[] offsets() {
    return this.offsets;
  }

  public long indexed() {
    return this.indexed;
  }

  public long nulls() {
    return this.bitset;
  }

  @Override
  public int numFields() {
    return this.offsets.length;
  }

  @Override
  public InternalRow copy() {
    int[] copyOffsets = new int[this.offsets.length];
    System.arraycopy(this.offsets, 0, copyOffsets, 0, this.offsets.length);
    IndexedRow row = new IndexedRow(this.indexed, copyOffsets);
    row.setNulls(this.bitset);
    // TODO: perform direct copy on byte buffer
    if (hasIndexRegion()) {
      row.setIndexRegion(this.indexBuffer.array(), this.indexBuffer.arrayOffset(),
        this.indexBuffer.array().length - this.indexBuffer.arrayOffset());
    }
    // TODO: perform direct copy on byte buffer
    if (hasDataRegion()) {
      row.setDataRegion(this.dataBuffer.array(), this.dataBuffer.arrayOffset(),
        this.dataBuffer.array().length - this.dataBuffer.arrayOffset());
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    return this.bitset != 0;
  }

  //////////////////////////////////////////////////////////////
  // Specialized getters
  //////////////////////////////////////////////////////////////

  @Override
  public boolean isNullAt(int ordinal) {
    return (this.bitset & (1L << ordinal)) != 0;
  }

  @Override
  public int getInt(int ordinal) {
    if ((this.indexed & (1L << ordinal)) != 0) {
      return this.indexBuffer.getInt(this.offsets[ordinal]);
    } else {
      return this.dataBuffer.getInt(this.offsets[ordinal]);
    }
  }

  @Override
  public long getLong(int ordinal) {
    if ((this.indexed & (1L << ordinal)) != 0) {
      return this.indexBuffer.getLong(this.offsets[ordinal]);
    } else {
      return this.dataBuffer.getLong(this.offsets[ordinal]);
    }
  }

  private UTF8String getUTF8String(int ordinal, ByteBuffer buf) {
    long metadata = buf.getLong(this.offsets[ordinal]);
    int offset = (int) (metadata >>> 32);
    int length = (int) (metadata & Integer.MAX_VALUE);
    byte[] bytes = new byte[length];
    int oldPos = buf.position();
    buf.position(offset);
    buf.get(bytes, 0, length);
    buf.position(oldPos);
    return UTF8String.fromBytes(bytes);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if ((this.indexed & 1L << ordinal) != 0) {
      return getUTF8String(ordinal, this.indexBuffer);
    } else {
      return getUTF8String(ordinal, this.dataBuffer);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        sb.append("null");
      } else {
        sb.append("<value>");
      }
      if (i < numFields() - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
