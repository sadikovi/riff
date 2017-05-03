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

package com.github.sadikovi.riff.ntree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.GenericInternalRow;
import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Stripe min/max statistics based on internal row.
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
    protected int[] values = new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE };

    IntStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      int value = row.getInt(ordinal);
      values[ORD_MIN] = Math.min(values[ORD_MIN], value);
      values[ORD_MAX] = Math.max(values[ORD_MAX], value);
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeInt(values[ORD_MIN]);
      buf.writeInt(values[ORD_MAX]);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      values[ORD_MIN] = buf.getInt();
      values[ORD_MAX] = buf.getInt();
    }

    @Override
    protected void merge(Statistics obj) {
      IntStatistics that = (IntStatistics) obj;
      values[ORD_MIN] = Math.min(that.values[ORD_MIN], values[ORD_MIN]);
      values[ORD_MAX] = Math.max(that.values[ORD_MAX], values[ORD_MAX]);
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public int getInt(int ordinal) {
      return values[ordinal];
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
      return Arrays.equals(values, that.values) && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "INT[hasNulls=" + hasNulls() + ", min=" + values[ORD_MIN] + ", max=" +
        values[ORD_MAX] + "]";
    }
  }

  /**
   * Long values statistics.
   */
  static class LongStatistics extends Statistics {
    public static final byte ID = 4;
    protected long[] values = new long[] { Long.MAX_VALUE, Long.MIN_VALUE };

    LongStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      long value = row.getLong(ordinal);
      values[ORD_MIN] = Math.min(values[ORD_MIN], value);
      values[ORD_MAX] = Math.max(values[ORD_MAX], value);
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      buf.writeLong(values[ORD_MIN]);
      buf.writeLong(values[ORD_MAX]);
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      values[ORD_MIN] = buf.getLong();
      values[ORD_MAX] = buf.getLong();
    }

    @Override
    protected void merge(Statistics obj) {
      LongStatistics that = (LongStatistics) obj;
      values[ORD_MIN] = Math.min(that.values[ORD_MIN], values[ORD_MIN]);
      values[ORD_MAX] = Math.max(that.values[ORD_MAX], values[ORD_MAX]);
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      // long statistics values are never null
      return false;
    }

    @Override
    public long getLong(int ordinal) {
      return values[ordinal];
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof LongStatistics)) return false;
      LongStatistics that = (LongStatistics) obj;
      if (that == this) return true;
      return Arrays.equals(values, that.values) && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "LONG[hasNulls=" + hasNulls() + ", min=" + values[ORD_MIN] + ", max=" +
        values[ORD_MAX] + "]";
    }
  }

  /**
   * UTF8String values statistics.
   */
  static class UTF8StringStatistics extends Statistics {
    public static final byte ID = 8;
    protected UTF8String[] values = new UTF8String[2];

    UTF8StringStatistics() {
      super(ID);
    }

    @Override
    protected void updateState(InternalRow row, int ordinal) {
      UTF8String value = row.getUTF8String(ordinal);
      if (values[ORD_MIN] == null) {
        values[ORD_MIN] = value;
      } else if (values[ORD_MIN].compareTo(value) > 0) {
        values[ORD_MIN] = value;
      }
      if (values[ORD_MAX] == null) {
        values[ORD_MAX] = value;
      } else if (values[ORD_MAX].compareTo(value) < 0) {
        values[ORD_MAX] = value;
      }
    }

    @Override
    protected void writeState(OutputBuffer buf) throws IOException {
      // write byte of indication if any data has been collected
      buf.writeByte(values[ORD_MIN] != null ? 1 : 0);
      // min and max are either both set or none
      if (values[ORD_MIN] != null) {
        // write min value
        byte[] bytes = values[ORD_MIN].getBytes();
        buf.writeInt(bytes.length);
        buf.write(bytes);
        // write max value
        bytes = values[ORD_MAX].getBytes();
        buf.writeInt(bytes.length);
        buf.write(bytes);
      }
    }

    @Override
    protected void readState(ByteBuffer buf) throws IOException {
      boolean isNull = buf.get() == 0;
      if (isNull) {
        values[ORD_MIN] = null;
        values[ORD_MAX] = null;
      } else {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        values[ORD_MIN] = UTF8String.fromBytes(bytes);

        len = buf.getInt();
        bytes = new byte[len];
        buf.get(bytes);
        values[ORD_MAX] = UTF8String.fromBytes(bytes);
      }
    }

    @Override
    protected void merge(Statistics obj) {
      UTF8StringStatistics that = (UTF8StringStatistics) obj;
      // update min
      if (values[ORD_MIN] == null || that.values[ORD_MIN] == null) {
        values[ORD_MIN] = values[ORD_MIN] == null ? that.values[ORD_MIN] : values[ORD_MIN];
      } else {
        values[ORD_MIN] = values[ORD_MIN].compareTo(that.values[ORD_MIN]) > 0 ?
          that.values[ORD_MIN] : values[ORD_MIN];
      }
      // update max
      if (values[ORD_MAX] == null || that.values[ORD_MAX] == null) {
        values[ORD_MAX] = values[ORD_MAX] == null ? that.values[ORD_MAX] : values[ORD_MAX];
      } else {
        values[ORD_MAX] = values[ORD_MAX].compareTo(that.values[ORD_MAX]) < 0 ?
          that.values[ORD_MAX] : values[ORD_MAX];
      }
      // update nulls
      this.hasNulls = this.hasNulls || that.hasNulls;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return values[ordinal] == null;
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return values[ordinal];
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof UTF8StringStatistics)) return false;
      UTF8StringStatistics that = (UTF8StringStatistics) obj;
      if (that == this) return true;
      return Arrays.equals(values, that.values) && that.hasNulls == this.hasNulls;
    }

    @Override
    public String toString() {
      return "UTF8[hasNulls=" + hasNulls() + ", min=" + values[ORD_MIN] + ", max=" +
        values[ORD_MAX] + "]";
    }
  }
}
