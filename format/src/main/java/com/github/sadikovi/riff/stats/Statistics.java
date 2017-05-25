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

package com.github.sadikovi.riff.stats;

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

import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.row.GenericInternalRow;

/**
 * Statistics keep information about min and max values and nulls.
 */
public abstract class Statistics extends GenericInternalRow {
  // ordinal for min value in a row
  public static final int ORD_MIN = 0;
  // ordinal for max value in a row
  public static final int ORD_MAX = 1;

  protected boolean hasNulls;

  Statistics() {
    this.hasNulls = false;
  }

  /**
   * Update value based on internal row and ordinal. Value is non-null, and has been updated with
   * `hasNulls` property.
   * @param row container for values
   * @param ordinal position to extract value at
   */
  protected abstract void updateNonNullValue(InternalRow row, int ordinal);

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
  public abstract void merge(Statistics obj);

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
    boolean isNull = row.isNullAt(ordinal);
    hasNulls = hasNulls || isNull;
    if (!isNull) {
      updateNonNullValue(row, ordinal);
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
    byte meta = (byte) (statisticsToMagic(this) | (hasNulls ? (byte) (1 << 7) : 0));
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
    Statistics stats = magicToStatistics(meta & 0x7f);
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
      return new UTF8Statistics();
    } else if (dataType instanceof DateType) {
      return new DateStatistics();
    } else if (dataType instanceof TimestampType) {
      return new TimestampStatistics();
    } else if (dataType instanceof BooleanType) {
      return new BooleanStatistics();
    } else if (dataType instanceof ShortType) {
      return new ShortStatistics();
    } else if (dataType instanceof ByteType) {
      return new ByteStatistics();
    } else {
      throw new UnsupportedOperationException("No statistics for data type: " + dataType);
    }
  }

  /**
   * Get magic byte for statistics.
   * @param stats statistics instance
   * @return byte to use for serialization
   */
  public static byte statisticsToMagic(Statistics stats) {
    if (stats instanceof IntStatistics) return 1;
    if (stats instanceof LongStatistics) return 2;
    if (stats instanceof UTF8Statistics) return 3;
    if (stats instanceof DateStatistics) return 4;
    if (stats instanceof TimestampStatistics) return 5;
    if (stats instanceof BooleanStatistics) return 6;
    if (stats instanceof ShortStatistics) return 7;
    if (stats instanceof ByteStatistics) return 8;
    throw new UnsupportedOperationException("Unrecognized statistics: " + stats);
  }

  /**
   * Get statistics instance for magic byte.
   * @param id magic byte
   * @return new statistics instance
   */
  public static Statistics magicToStatistics(int magic) {
    switch (magic) {
      case 1:
        return new IntStatistics();
      case 2:
        return new LongStatistics();
      case 3:
        return new UTF8Statistics();
      case 4:
        return new DateStatistics();
      case 5:
        return new TimestampStatistics();
      case 6:
        return new BooleanStatistics();
      case 7:
        return new ShortStatistics();
      case 8:
        return new ByteStatistics();
      default:
        throw new UnsupportedOperationException("Unrecognized statistics: " + magic);
    }
  }
}
