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
 * Short values statistics.
 */
class ShortStatistics extends Statistics {
  protected short min = Short.MAX_VALUE;
  protected short max = Short.MIN_VALUE;

  ShortStatistics() {
    super();
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
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
  public void merge(Statistics obj) {
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
    return "SHORT[hasNulls=" + hasNulls + ", min=" + min + ", max=" + max + "]";
  }
}
