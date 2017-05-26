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

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Timestamp values statistics.
 * Derived from [[LongStatistics]], because Spark stores timestamps as longs internally.
 */
class TimestampStatistics extends Statistics {
  protected long min = Long.MAX_VALUE;
  protected long max = Long.MIN_VALUE;

  TimestampStatistics() {
    super();
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
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
  public void merge(Statistics obj) {
    TimestampStatistics that = (TimestampStatistics) obj;
    TimestampStatistics res = new TimestampStatistics();
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
    // timestamp statistics values are never null
    return false;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TimestampStatistics)) return false;
    TimestampStatistics that = (TimestampStatistics) obj;
    if (that == this) return true;
    return that.min == this.min && that.max == this.max && that.hasNulls == this.hasNulls;
  }

  @Override
  public String toString() {
    return "TIME[hasNulls=" + hasNulls + ", min=" + min + ", max=" + max + "]";
  }
}
