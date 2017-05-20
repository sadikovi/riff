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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Stripe min/max statistics.
 * Keeps information about min and max values and nulls.
 */
public abstract class Statistics extends GenericInternalRow {
  // ordinal for min value in a row
  public static final int ORD_MIN = 0;
  // ordinal for max value in a row
  public static final int ORD_MAX = 1;

  protected final byte id;
  protected boolean hasNulls;

  Statistics(byte id) {
    if (id < 0) throw new IllegalArgumentException("Negative id: " + id);
    this.id = id;
    this.hasNulls = false;
  }

  /**
   * Update value based on internal row and ordinal.
   * Value is guaranteed to be non-null.
   * @param row container for values
   * @param ordinal position to extract value at
   */
  protected abstract void updateState(InternalRow row, int ordinal);

  /**
   * Write current state of statistics into output buffer.
   * @param buf output buffer
   * @throws IOException
   */
  protected abstract void writeState(OutputBuffer buf) throws IOException;

  /**
   * Read current state of statistics from byte buffer, buffer is guaranteed to contain all data,
   * necessary to reconstruct statistics. Calling this method should overwrite any existing state.
   * @param buf byte buffer
   * @throws IOException
   */
  protected abstract void readState(ByteBuffer buf) throws IOException;

  /**
   * Merge `that` statistics instance into this instance. `that` instance should not be modified.
   * It is guaranteed that provided statistics instance will be of the same type as this one.
   * @param obj instance to merge
   */
  protected abstract void merge(Statistics obj);

  /**
   * Return unique statistics id.
   * @return id
   */
  public byte id() {
    return id;
  }

  /**
   * Whether or not this statistics instance has null values.
   * @return true if instance has null values, false otherwise
   */
  public boolean hasNulls() {
    return hasNulls;
  }

  /**
   * Update statistics for this row and ordinal, value at ordinal can be null
   * @param row internal row with values
   * @param ordinal position to read
   */
  public void update(InternalRow row, int ordinal) {
    hasNulls = hasNulls || row.isNullAt(ordinal);
    if (!row.isNullAt(ordinal)) {
      updateState(row, ordinal);
    }
  }

  /**
   * Write statistics instance into output buffer.
   * This writes statistics id first, so we can deserialize it for a specific type.
   * @param buf output buffer
   * @throws IOException
   */
  public void writeExternal(OutputBuffer buf) throws IOException {
    // metadata consists of id and nulls flag in single byte
    byte meta = (byte) (id | (hasNulls ? (byte) (1 << 7) : 0));
    buf.writeByte(meta);
    writeState(buf);
  }

  /**
   * Read statistics from byte buffer.
   * @param buf byte buffer with statistics data
   * @throws IOException
   */
  public static Statistics readExternal(ByteBuffer buf) throws IOException {
    // read byte of metadata
    byte meta = buf.get();
    boolean hasNulls = (meta & 1 << 7) != 0;
    int id = meta & 0x7f;
    Statistics stats = null;
    if (id == NoopStatistics.ID) {
      stats = new NoopStatistics();
    } else if (id == IntStatistics.ID) {
      stats = new IntStatistics();
    } else if (id == LongStatistics.ID) {
      stats = new LongStatistics();
    } else if (id == UTF8StringStatistics.ID) {
      stats = new UTF8StringStatistics();
    } else if (id == BooleanStatistics.ID) {
      stats = new BooleanStatistics();
    } else if (id == ShortStatistics.ID) {
      stats = new ShortStatistics();
    } else if (id == ByteStatistics.ID) {
      stats = new ByteStatistics();
    } else {
      throw new IOException("Unrecognized statistics id: " + id);
    }
    stats.readState(buf);
    stats.hasNulls = hasNulls;
    return stats;
  }

  /**
   * Return new statistics instance for specified type. If type is unsupported, no-op statistics
   * are returned.
   * @param dataType SQL type for the value
   * @return statistics instance
   */
  public static Statistics sqlTypeToStatistics(DataType dataType) {
    if (dataType instanceof IntegerType) {
      return new IntStatistics();
    } else if (dataType instanceof LongType) {
      return new LongStatistics();
    } else if (dataType instanceof StringType) {
      return new UTF8StringStatistics();
    } else if (dataType instanceof DateType) {
      return new IntStatistics();
    } else if (dataType instanceof TimestampType) {
      return new LongStatistics();
    } else if (dataType instanceof BooleanType) {
      return new BooleanStatistics();
    } else if (dataType instanceof ShortType) {
      return new ShortStatistics();
    } else if (dataType instanceof ByteType) {
      return new ByteStatistics();
    } else {
      return new NoopStatistics();
    }
  }

  /**
   * Noop statistics are created when data type is unknown or unsupported. Such statistics always
   * yield true, when asked if value is in range, because they do not any information about data.
   * Note that statistics still collect information about null values regardless.
   */
  static class NoopStatistics extends Statistics {
    public static final byte ID = 1;

    NoopStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) { /* no-op */ }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException { /* no-op */ }

    @Override
    protected void readState(ByteBuffer buf) throws IOException { /* no-op */ }

    @Override
    protected void merge(Statistics obj) {
      this.hasNulls = this.hasNulls || obj.hasNulls;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof NoopStatistics)) return false;
      NoopStatistics that = (NoopStatistics) obj;
      if (that == this) return true;
      return that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "NOOP[hasNulls=" + hasNulls() + "]";
    }
  }

  /**
   * Integer values statistics.
   */
  static class IntStatistics extends Statistics {
    public static final byte ID = 2;

    protected int min = Integer.MAX_VALUE;
    protected int max = Integer.MIN_VALUE;

    IntStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      int value = row.getInt(ordinal);
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeInt(min);
      buf.writeInt(max);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      min = buf.getInt();
      max = buf.getInt();
    }

    @Override
    protected void merge(Statistics obj) {
      IntStatistics that = (IntStatistics) obj;
      this.min = Math.min(that.min, this.min);
      this.max = Math.max(that.max, this.max);
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public int getInt(int ordinal) {
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // int statistics values are never null
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof IntStatistics)) return false;
      IntStatistics that = (IntStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "INT[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }

  /**
   * Long values statistics.
   */
  static class LongStatistics extends Statistics {
    public static final byte ID = 4;

    protected long min = Long.MAX_VALUE;
    protected long max = Long.MIN_VALUE;

    LongStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      long value = row.getLong(ordinal);
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeLong(min);
      buf.writeLong(max);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      min = buf.getLong();
      max = buf.getLong();
    }

    @Override
    protected void merge(Statistics obj) {
      LongStatistics that = (LongStatistics) obj;
      LongStatistics res = new LongStatistics();
      this.min = Math.min(that.min, this.min);
      this.max = Math.max(that.max, this.max);
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public long getLong(int ordinal) {
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // long statistics values are never null
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof LongStatistics)) return false;
      LongStatistics that = (LongStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "LONG[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }

  /**
   * UTF8String values statistics.
   */
  static class UTF8StringStatistics extends Statistics {
    public static final byte ID = 8;

    protected UTF8String min = null;
    protected UTF8String max = null;

    UTF8StringStatistics() {
      super(ID);
    }

    /**
     * Return deep copy of UTF8String, this method forcefully copies `getBytes()` bytes in
     * UTF8String, since it does not return copy when backed by single array.
     * Clone is null safe, and would return null for null input.
     * @param str UTF8 string to clone
     * @return copy
     */
    private UTF8String clone(UTF8String str) {
      if (str == null) return null;
      byte[] bytes = new byte[str.numBytes()];
      System.arraycopy(str.getBytes(), 0, bytes, 0, bytes.length);
      return UTF8String.fromBytes(bytes);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      UTF8String value = row.getUTF8String(ordinal);
      // only clone on actual update
      min = (min == null) ? clone(value) : (min.compareTo(value) > 0 ? clone(value) : min);
      max = (max == null) ? clone(value) : (max.compareTo(value) < 0 ? clone(value) : max);
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      // write byte of indication if any data has been collected
      buf.writeByte(min != null ? 1 : 0);
      // min and max are either both set or none
      if (min != null) {
        // write min value
        byte[] bytes = min.getBytes();
        buf.writeInt(bytes.length);
        buf.write(bytes);
        // write max value
        bytes = max.getBytes();
        buf.writeInt(bytes.length);
        buf.write(bytes);
      }
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      boolean isNull = buf.get() == 0;
      if (isNull) {
        min = null;
        max = null;
      } else {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        min = UTF8String.fromBytes(bytes);

        len = buf.getInt();
        bytes = new byte[len];
        buf.get(bytes);
        max = UTF8String.fromBytes(bytes);
      }
    }

    @Override
    protected void merge(Statistics obj) {
      UTF8StringStatistics that = (UTF8StringStatistics) obj;
      // update min
      if (this.min == null || that.min == null) {
        this.min = this.min == null ? clone(that.min) : this.min;
      } else {
        this.min = this.min.compareTo(that.min) > 0 ? clone(that.min) : this.min;
      }
      // update max
      if (this.max == null || that.max == null) {
        this.max = this.max == null ? clone(that.max) : this.max;
      } else {
        this.max = this.max.compareTo(that.max) < 0 ? clone(that.max) : this.max;
      }
      // update nulls
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      if (ordinal == ORD_MIN) return min == null;
      if (ordinal == ORD_MAX) return max == null;
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof UTF8StringStatistics)) return false;
      UTF8StringStatistics that = (UTF8StringStatistics) obj;
      if (that == this) return true;
      if (that.hasNulls() != this.hasNulls()) return false;
      boolean compareMin = (that.min == null && this.min == null) ||
        (that.min != null && this.min != null && that.min.equals(this.min));
      boolean compareMax = (that.max == null && this.max == null) ||
        (that.max != null && this.max != null && that.max.equals(this.max));
      return compareMin && compareMax;
    }

    @Override
    public String toString() {
      return "UTF8[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }

  /**
   * Boolean values statistics.
   */
  static class BooleanStatistics extends Statistics {
    public static final byte ID = 16;

    // whether or not statistics have min/max set
    protected boolean hasValues = false;
    // associated with min value
    protected boolean min = true;
    // associated with max value
    protected boolean max = false;

    BooleanStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      // value is guaranteed to be non-null
      boolean value = row.getBoolean(ordinal);
      if (hasValues) {
        // if value is true, set max to true, otherwise set min to false
        if (value) {
          max = value;
        } else {
          min = value;
        }
      } else {
        // for this update we have situation when min = max = true or min = max = false
        min = value;
        max = value;
        hasValues = true;
      }
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      // write 3 most significant bits as state of statistics
      int flags = hasValues ? 1 : 0;
      // set flags anyway, we will only read them if hasValues is set
      flags |= min ? 2 : 0;
      flags |= max ? 4 : 0;
      buf.writeByte(flags);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      byte flags = buf.get();
      hasValues = (flags & 1) != 0;
      // flags are not considered if hasValues is set to false
      min = (flags & 2) != 0;
      max = (flags & 4) != 0;
    }

    @Override
    protected void merge(Statistics obj) {
      BooleanStatistics that = (BooleanStatistics) obj;
      if (this.hasValues) {
        this.min = that.hasValues ? (that.min && this.min) : this.min;
        this.max = that.hasValues ? (that.max || this.max) : this.max;
      } else {
        // at this point values we assign do not really matter, because in case that.hasValues is
        // false, then we will never check min/max values, otherwise they will be set correctly
        this.min = that.min;
        this.max = that.max;
      }
      this.hasValues = this.hasValues || that.hasValues;
      // update nulls
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      if (!hasValues) throw new IllegalStateException("Boolean statistics are not set");
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // if boolean statistics are not set, return false, because there is no distinction between
      // states for boolean statistics
      return !hasValues;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof BooleanStatistics)) return false;
      BooleanStatistics that = (BooleanStatistics) obj;
      if (that == this) return true;
      return this.hasNulls == that.hasNulls &&
        this.hasValues == that.hasValues &&
        this.min == that.min &&
        this.max == that.max;
    }

    @Override
    public String toString() {
      return "BOOL[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }

  /**
   * Short values statistics.
   */
  static class ShortStatistics extends Statistics {
    public static final byte ID = 32;

    protected short min = Short.MAX_VALUE;
    protected short max = Short.MIN_VALUE;

    ShortStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      short value = row.getShort(ordinal);
      min = (min > value) ? value : min;
      max = (max < value) ? value : max;
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeShort(min);
      buf.writeShort(max);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      min = buf.getShort();
      max = buf.getShort();
    }

    @Override
    protected void merge(Statistics obj) {
      ShortStatistics that = (ShortStatistics) obj;
      this.min = (that.min < this.min) ? that.min : this.min;
      this.max = (that.max > this.max) ? that.max : this.max;
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public short getShort(int ordinal) {
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // short statistics values are never null
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ShortStatistics)) return false;
      ShortStatistics that = (ShortStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "SHORT[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }

  /**
   * Byte values statistics.
   */
  static class ByteStatistics extends Statistics {
    public static final byte ID = 64;

    protected byte min = Byte.MAX_VALUE;
    protected byte max = Byte.MIN_VALUE;

    ByteStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      byte value = row.getByte(ordinal);
      min = (min > value) ? value : min;
      max = (max < value) ? value : max;
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeByte(min);
      buf.writeByte(max);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      min = buf.get();
      max = buf.get();
    }

    @Override
    protected void merge(Statistics obj) {
      ByteStatistics that = (ByteStatistics) obj;
      this.min = (that.min < this.min) ? that.min : this.min;
      this.max = (that.max > this.max) ? that.max : this.max;
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public byte getByte(int ordinal) {
      if (ordinal == ORD_MIN) return min;
      if (ordinal == ORD_MAX) return max;
      throw new UnsupportedOperationException("Invalid ordinal " + ordinal);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // byte statistics values are never null
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ByteStatistics)) return false;
      ByteStatistics that = (ByteStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "BYTE[hasNulls=" + hasNulls() + ", min=" + min + ", max=" + max + "]";
    }
  }
}
