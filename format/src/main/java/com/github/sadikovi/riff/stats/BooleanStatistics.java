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
  // associated with min value
  protected Boolean min = null;
  // associated with max value
  protected Boolean max = null;

  BooleanStatistics() {
    super();
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
    boolean value = row.getBoolean(ordinal);
    min = (min == null) ? value : (min.compareTo(value) > 0 ? value : min);
    max = (max == null) ? value : (max.compareTo(value) < 0 ? value : max);
  }

  @Override
  protected void writeState(OutputBuffer buf) throws IOException {
    // write 3 most significant bits as state of statistics
    // min and max are both null, or both set
    int flags = min == null ? 1 : 0;
    flags |= (min != null && min.booleanValue()) ? 2 : 0;
    flags |= (max != null && max.booleanValue()) ? 4 : 0;
    buf.writeByte(flags);
  }

  @Override
  protected void readState(ByteBuffer buf) throws IOException {
    byte flags = buf.get();
    if ((flags & 1) != 0) {
      min = (flags & 2) != 0;
      max = (flags & 4) != 0;
    } else {
      min = null;
      max = null;
    }
  }

  @Override
  protected void merge(Statistics obj) {
    BooleanStatistics that = (BooleanStatistics) obj;
    if (that.min == null || that.max == null) {
      this.min = that.min != null ? (that.min && this.min) : this.min;
      this.max = that.max != null ? (that.max || this.max) : this.max;
    } else {
      // at this point values we assign do not really matter, just set to 'that' values
      this.min = that.min;
      this.max = that.max;
    }
    // update nulls
    this.hasNulls = this.hasNulls || that.hasNulls;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    if (min == null || max == null) {
      throw new IllegalStateException("Boolean statistics are not set");
    }
    if (ordinal == ORD_MIN) return min.booleanValue();
    if (ordinal == ORD_MAX) return max.booleanValue();
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
    if (obj == null || !(obj instanceof BooleanStatistics)) return false;
    BooleanStatistics that = (BooleanStatistics) obj;
    if (that == this) return true;
    boolean sameMin = (min == null && that.min == null) ||
      (min != null && that.min != null && min.equals(that.min));
    boolean sameMax = (max == null && that.max == null) ||
      (max != null && that.max != null && max.equals(that.max));
    return hasNulls == that.hasNulls && sameMin && sameMax;
  }

  @Override
  public String toString() {
    return "BOOL[hasNulls=" + hasNulls + ", min=" + min + ", max=" + max + "]";
  }
}
