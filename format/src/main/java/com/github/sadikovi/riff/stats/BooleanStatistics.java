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
 * Boolean values statistics.
 */
class BooleanStatistics extends Statistics {
  // whether or not statistics have min/max set
  protected boolean hasValues = false;
  // associated with min value
  protected boolean min = true;
  // associated with max value
  protected boolean max = false;

  BooleanStatistics() {
    super();
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
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
  public void merge(Statistics obj) {
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
