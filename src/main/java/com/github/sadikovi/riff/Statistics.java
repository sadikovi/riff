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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.tree.BoundReference;

/**
 * Stripe min/max statistics.
 * Keeps information about min and max values and nulls.
 */
public abstract class Statistics {
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
   * Evaluate bound reference (name and ordinal) based on current statistics instance.
   * Should return true if node value is within statistics and false otherwise. Most of the time it
   * is useful to call method on node to evaluate it with min/max/null.
   * @param ref bound reference to evaluate
   * @return true if value is within range, false otherwise (similar with null values)
   */
  public abstract boolean evaluateState(BoundReference ref);

  /**
   * Convert content of this statistics into internal row. Each such row contains two fields:
   * [min, max] which are type dependent. `nulls` field is always boolean, min/max depends on
   * statistics type. Ordinals are provided as global constants for access.
   */
  public abstract InternalRow toRow();

  /**
   * Return unique statistics id.
   * @return id
   */
  public byte id() {
    return id;
  }

  /**
   * Get min value for this statistics. Used for testing purposes only.
   * Recommended that implementations overwrite this method.
   * @return min value
   */
  public Object getMin() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get max value for this statistics. Used for testing purposes only.
   * Recommended that implementations overwrite this method.
   * @return max value
   */
  public Object getMax() {
    throw new UnsupportedOperationException();
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
    } else {
      return new NoopStatistics();
    }
  }

  /**
   * [[StatisticsRow]] is a container to store min/max values for each statistics type to be used
   * in evaluating predicate.
   */
  static class StatisticsRow extends GenericInternalRow {
    private int[] intValues;
    private long[] longValues;
    private UTF8String[] utf8Values;

    protected StatisticsRow() { /* no-op */ }

    protected StatisticsRow(int min, int max) {
      intValues = new int[2];
      intValues[ORD_MIN] = min;
      intValues[ORD_MAX] = max;
    }

    protected StatisticsRow(long min, long max) {
      longValues = new long[2];
      longValues[ORD_MIN] = min;
      longValues[ORD_MAX] = max;
    }

    protected StatisticsRow(UTF8String min, UTF8String max) {
      utf8Values = new UTF8String[2];
      utf8Values[ORD_MIN] = min;
      utf8Values[ORD_MAX] = max;
    }

    @Override
    public int getInt(int ordinal) {
      return intValues[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
      return longValues[ordinal];
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return utf8Values[ordinal];
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
    public boolean evaluateState(BoundReference ref) {
      // statistics only contain information about nullability
      return ref.statUpdate(hasNulls);
    }

    @Override
    public InternalRow toRow() {
      return new StatisticsRow();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof NoopStatistics)) return false;
      NoopStatistics that = (NoopStatistics) obj;
      if (that == this) return true;
      return that.hasNulls() == this.hasNulls();
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
    public Object getMin() {
      return min;
    }

    @Override
    public Object getMax() {
      return max;
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
    public boolean evaluateState(BoundReference ref) {
      return ref.statUpdate(hasNulls) && ref.statUpdate(min, max);
    }

    @Override
    public InternalRow toRow() {
      return new StatisticsRow(min, max);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof IntStatistics)) return false;
      IntStatistics that = (IntStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls() == this.hasNulls();
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
    public Object getMin() {
      return min;
    }

    @Override
    public Object getMax() {
      return max;
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
    public boolean evaluateState(BoundReference ref) {
      return ref.statUpdate(hasNulls) && ref.statUpdate(min, max);
    }

    @Override
    public InternalRow toRow() {
      return new StatisticsRow(min, max);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof LongStatistics)) return false;
      LongStatistics that = (LongStatistics) obj;
      if (that == this) return true;
      return that.min == this.min && that.max == this.max && that.hasNulls() == this.hasNulls();
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

    @Override
    public Object getMin() {
      return min;
    }

    @Override
    public Object getMax() {
      return max;
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      UTF8String value = row.getUTF8String(ordinal);
      min = (min == null) ? value : (min.compareTo(value) > 0 ? value : min);
      max = (max == null) ? value : (max.compareTo(value) < 0 ? value : max);
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
        this.min = this.min == null ? that.min : this.min;
      } else {
        this.min = this.min.compareTo(that.min) > 0 ? that.min : this.min;
      }
      // update max
      if (this.max == null || that.max == null) {
        this.max = this.max == null ? that.max : this.max;
      } else {
        this.max = this.max.compareTo(that.max) < 0 ? that.max : this.max;
      }
      // update nulls
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public boolean evaluateState(BoundReference ref) {
      return ref.statUpdate(hasNulls) && ref.statUpdate(min, max);
    }

    @Override
    public InternalRow toRow() {
      return new StatisticsRow(min, max);
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
}
