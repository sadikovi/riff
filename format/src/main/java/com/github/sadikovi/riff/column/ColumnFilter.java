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

package com.github.sadikovi.riff.column;

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
 * [[ColumnFilter]] class is a set that can resolve predicates with higher selectivity than
 * statistics and should be considered optional optimization.
 * Current implementation uses Bloom filter to report if value *is not* in the stripe.
 * Subclasses should always provide default constructor for serialization.
 */
public abstract class ColumnFilter {
  private boolean hasNulls = false;

  // == "mightContain" methods ==
  // Filter might contain value of this bound reference
  // If filter contains value or it is unknown - return `true`, in other cases return `false`.

  public boolean mightContain(boolean value) {
    throw new UnsupportedOperationException();
  }

  public boolean mightContain(byte value) {
    throw new UnsupportedOperationException();
  }

  public boolean mightContain(short value) {
    throw new UnsupportedOperationException();
  }

  public boolean mightContain(int value) {
    throw new UnsupportedOperationException();
  }

  public boolean mightContain(long value) {
    throw new UnsupportedOperationException();
  }

  public boolean mightContain(UTF8String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Update column filter with guaranteed non-null value at provided ordinal.
   * @param row internal row for update
   * @param ordinal position for update
   */
  public abstract void updateNonNullValue(InternalRow row, int ordinal);

  /**
   * Write state of this filter into output buffer.
   * There is not need to write id, it will be written automatically by `writeExternal`.
   * @param buffer output buffer
   * @throws IOException
   */
  protected abstract void writeState(OutputBuffer buffer) throws IOException;

  /**
   * Read state for this filter.
   * Method should update any internal state that filter has so far.
   * @param buffer byte buffer
   * @throws IOException
   */
  protected abstract void readState(ByteBuffer buffer) throws IOException;

  /**
   * When true, column filter contains null values, false otherwise.
   * @return indicator of whether or not column filter has null values
   */
  public boolean hasNulls() {
    return hasNulls;
  }

  /**
   * Update column filter with value in internal row at ordinal
   * @param row row to use for update
   * @param ordinal position of the value
   */
  public final void update(InternalRow row, int ordinal) {
    boolean isNull = row.isNullAt(ordinal);
    hasNulls = hasNulls || isNull;
    if (!isNull) {
      updateNonNullValue(row, ordinal);
    }
  }

  /**
   * Write this filter into output buffer.
   * @param output buffer
   * @throws IOException
   */
  public final void writeExternal(OutputBuffer buffer) throws IOException {
    byte meta = (byte) (columnFilterToMagic(this) | (hasNulls ? (byte) (1 << 7) : 0));
    buffer.writeByte(meta);
    writeState(buffer);
  }

  /**
   * Deserialize filter from byte buffer.
   * @param buf byte buffer with filter data
   * @return filter
   * @throws IOException
   */
  public static ColumnFilter readExternal(ByteBuffer buf) throws IOException {
    // read byte of metadata
    byte meta = buf.get();
    boolean hasNulls = (meta & 1 << 7) != 0;
    ColumnFilter filter = magicToColumnFilter(meta & 0x7f);
    filter.readState(buf);
    filter.hasNulls = hasNulls;
    return filter;
  }

  /**
   * Return new noop column filter.
   * @return noop filter
   */
  public static ColumnFilter noopFilter() {
    return new NoopColumnFilter();
  }

  /**
   * Select column filter for SQL data type.
   * @param dataType Spark SQL data type
   * @param numItems expected number of items
   * @return column filter
   */
  public static ColumnFilter sqlTypeToColumnFilter(DataType dataType, int numItems) {
    if (dataType instanceof BooleanType) {
      return new BooleanColumnFilter();
    } else if (dataType instanceof ByteType) {
      return new ByteColumnFilter();
    } else if (dataType instanceof ShortType) {
      return new ShortColumnFilter();
    } else if (dataType instanceof IntegerType) {
      return new IntColumnFilter(numItems);
    } else if (dataType instanceof LongType) {
      return new LongColumnFilter(numItems);
    } else if (dataType instanceof StringType) {
      return new UTF8ColumnFilter(numItems);
    } else if (dataType instanceof DateType) {
      return new DateColumnFilter(numItems);
    } else if (dataType instanceof TimestampType) {
      return new TimestampColumnFilter(numItems);
    } else {
      return noopFilter();
    }
  }

  /**
   * Return unique magic byte assigned to each registered column filter.
   * @param filter column filter
   * @return magic
   */
  public static byte columnFilterToMagic(ColumnFilter filter) {
    if (filter instanceof NoopColumnFilter) return 1;
    if (filter instanceof BooleanColumnFilter) return 2;
    if (filter instanceof ByteColumnFilter) return 3;
    if (filter instanceof ShortColumnFilter) return 4;
    if (filter instanceof IntColumnFilter) return 5;
    if (filter instanceof LongColumnFilter) return 6;
    if (filter instanceof UTF8ColumnFilter) return 7;
    if (filter instanceof DateColumnFilter) return 8;
    if (filter instanceof TimestampColumnFilter) return 9;
    throw new UnsupportedOperationException("Unrecognized column filter: " + filter);
  }

  /**
   * Return instance of column filter for a magic.
   * @param magic unique magic byte
   * @return default filter instance
   */
  public static ColumnFilter magicToColumnFilter(int magic) {
    switch (magic) {
      case 1:
        return new NoopColumnFilter();
      case 2:
        return new BooleanColumnFilter();
      case 3:
        return new ByteColumnFilter();
      case 4:
        return new ShortColumnFilter();
      case 5:
        return new IntColumnFilter();
      case 6:
        return new LongColumnFilter();
      case 7:
        return new UTF8ColumnFilter();
      case 8:
        return new DateColumnFilter();
      case 9:
        return new TimestampColumnFilter();
      default:
        throw new UnsupportedOperationException("Unrecognized column filter magic: " + magic);
    }
  }
}
