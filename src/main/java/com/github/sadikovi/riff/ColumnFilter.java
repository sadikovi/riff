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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.sketch.BloomFilter;

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * [[ColumnFilter]] class is a set that can resolve predicates with higher selectivity than
 * statistics and should be considered optional optimization.
 */
public abstract class ColumnFilter {
  protected final byte id;

  protected ColumnFilter(byte id) {
    this.id = id;
  }

  /**
   * Filter might contain value of this bound reference.
   * If filter contains value or it is unknown - return `true`, in other cases return `false`.
   * This is similar to bloom filters.
   * @param value
   * @return true if node passes filter or unknown, false if node does not pass filter.
   */
  public abstract boolean mightContain(int value);
  public abstract boolean mightContain(long value);
  public abstract boolean mightContain(UTF8String value);

  /**
   * Update column filter with value in internal row at ordinal
   * @param row row to use for update
   * @param ordinal position of the value
   */
  public abstract void update(InternalRow row, int ordinal);

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
   * Select filter based on its byte id. Throws exception if id is not valid.
   * @param id filter id
   * @return filter instance
   */
  protected static ColumnFilter select(byte id) {
    switch (id) {
      case NoopColumnFilter.ID:
        return new NoopColumnFilter();
      case BloomColumnFilter.ID:
        return new BloomColumnFilter();
      default:
        throw new UnsupportedOperationException("Unknown id: " + id);
    }
  }

  /**
   * Return new noop column filter.
   * @return noop filter
   */
  public static ColumnFilter noopFilter() {
    return new NoopColumnFilter();
  }

  /**
   * Select typed bloom column filter.
   * @param dataType Spark SQL data type
   * @param numItems expected number of items
   * @return bloom column filter
   */
  public static ColumnFilter sqlTypeToBloomFilter(DataType dataType, int numItems) {
    if (dataType instanceof IntegerType) {
      return new BloomColumnFilter(numItems) {
        @Override
        public void update(InternalRow row, int ordinal) {
          filter.putLong(row.getInt(ordinal));
        }
      };
    } else if (dataType instanceof LongType) {
      return new BloomColumnFilter(numItems) {
        @Override
        public void update(InternalRow row, int ordinal) {
          filter.putLong(row.getLong(ordinal));
        }
      };
    } else if (dataType instanceof StringType) {
      return new BloomColumnFilter(numItems) {
        @Override
        public void update(InternalRow row, int ordinal) {
          filter.putBinary(row.getUTF8String(ordinal).getBytes());
        }
      };
    } else {
      throw new UnsupportedOperationException("Type for bloom column filter: " + dataType);
    }
  }

  /**
   * Deserialize filter from byte buffer.
   * @param buffer byte buffer with filter data
   * @return filter
   * @throws IOException
   */
  public static ColumnFilter readExternal(ByteBuffer buffer) throws IOException {
    ColumnFilter filter = select(buffer.get());
    filter.readState(buffer);
    return filter;
  }

  /**
   * Write this filter into output buffer.
   * @param output buffer
   * @throws IOException
   */
  public final void writeExternal(OutputBuffer buffer) throws IOException {
    buffer.writeByte(id);
    writeState(buffer);
  }

  /**
   * [[NoopColumnFilter]] represents null filter for fields that do not support filters.
   * Instead of storing null in a filter array, we use noop filter. It always evaluates to `true`.
   */
  static class NoopColumnFilter extends ColumnFilter {
    public static final byte ID = 1;

    NoopColumnFilter() {
      super(ID);
    }

    @Override
    public void update(InternalRow row, int ordinal) {
      // NoopColumnFilter does not update internal state
    }

    @Override
    public boolean mightContain(int value) {
      return true;
    }

    @Override
    public boolean mightContain(long value) {
      return true;
    }

    @Override
    public boolean mightContain(UTF8String value) {
      return true;
    }

    @Override
    protected void writeState(OutputBuffer buffer) throws IOException {
      // NoopColumnFilter does not have any state, no bytes are written input output.
    }

    @Override
    protected void readState(ByteBuffer buffer) throws IOException {
      // NoopColumnFilter does not have any state, no bytes are read from buffer.
    }
  }

  /**
   * [[BloomColumnFilter]] is backed by `BloomFilter` implementation in Spark sketch module.
   * Since it can take a fair amount of bytes to serialize, should be used for index fields only.
   * Each subclass must overwrite method to update filter for a given data type.
   */
  static class BloomColumnFilter extends ColumnFilter {
    public static final byte ID = 125;
    // default false positive probability
    private static final double DEFAULT_FPP = 0.5;
    // default number of records
    private static final long DEFAULT_EXPECTED_ITEMS = 16;
    // maximum number of items in filter
    private static final long MAX_EXPECTED_ITEMS = 100000;

    protected BloomFilter filter;

    BloomColumnFilter() {
      this(DEFAULT_EXPECTED_ITEMS, DEFAULT_FPP);
    }

    BloomColumnFilter(long numItems) {
      this(numItems, DEFAULT_FPP);
    }

    BloomColumnFilter(long numItems, double fpp) {
      super(ID);
      this.filter = BloomFilter.create(Math.min(numItems, MAX_EXPECTED_ITEMS), fpp);
    }

    @Override
    public void update(InternalRow row, int ordinal) {
      throw new RuntimeException("Column filter is in read-only state");
    }

    @Override
    public boolean mightContain(int value) {
      return filter.mightContainLong(value);
    }

    @Override
    public boolean mightContain(long value) {
      return filter.mightContainLong(value);
    }

    @Override
    public boolean mightContain(UTF8String value) {
      return filter.mightContainBinary(value.getBytes());
    }

    @Override
    protected void writeState(OutputBuffer buffer) throws IOException {
      // we have to create separate buffer to write bytes so we can read from byte buffer
      OutputBuffer tmp = new OutputBuffer();
      filter.writeTo(tmp);
      buffer.writeInt(tmp.bytesWritten());
      buffer.writeBytes(tmp.array());
    }

    @Override
    protected void readState(ByteBuffer buffer) throws IOException {
      int len = buffer.getInt();
      ByteArrayInputStream in = new ByteArrayInputStream(buffer.array(),
        buffer.arrayOffset() + buffer.position(), len);
      filter = BloomFilter.readFrom(in);
      buffer.position(buffer.position() + len);
    }
  }
}
