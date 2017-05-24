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
 * Integer values statistics.
 */
class IntStatistics extends Statistics {
  protected int min = Integer.MAX_VALUE;
  protected int max = Integer.MIN_VALUE;

  IntStatistics() {
    super();
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
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
    return "INT[hasNulls=" + hasNulls + ", min=" + min + ", max=" + max + "]";
  }
}
