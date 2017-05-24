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
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * UTF8String values statistics.
 */
class UTF8Statistics extends Statistics {
  protected UTF8String min = null;
  protected UTF8String max = null;

  UTF8Statistics() {
    super();
  }

  /**
   * Return deep copy of UTF8String, this method forcefully copies `getBytes()` bytes in
   * UTF8String, since it does not return copy when backed by single array.
   * Clone is null safe, and would return null for null input.
   * @param str UTF8 string to clone
   * @return copy
   */
  private UTF8String clone(UTF8String str) {
    if (str == null) return null;
    byte[] bytes = new byte[str.numBytes()];
    System.arraycopy(str.getBytes(), 0, bytes, 0, bytes.length);
    return UTF8String.fromBytes(bytes);
  }

  @Override
  protected void updateNonNullValue(InternalRow row, int ordinal) {
    UTF8String value = row.getUTF8String(ordinal);
    // only clone on actual update
    min = (min == null) ? clone(value) : (min.compareTo(value) > 0 ? clone(value) : min);
    max = (max == null) ? clone(value) : (max.compareTo(value) < 0 ? clone(value) : max);
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
    UTF8Statistics that = (UTF8Statistics) obj;
    // update min
    if (this.min == null || that.min == null) {
      this.min = this.min == null ? clone(that.min) : this.min;
    } else {
      this.min = this.min.compareTo(that.min) > 0 ? clone(that.min) : this.min;
    }
    // update max
    if (this.max == null || that.max == null) {
      this.max = this.max == null ? clone(that.max) : this.max;
    } else {
      this.max = this.max.compareTo(that.max) < 0 ? clone(that.max) : this.max;
    }
    // update nulls
    this.hasNulls = this.hasNulls || that.hasNulls;
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (ordinal == ORD_MIN) return min;
    if (ordinal == ORD_MAX) return max;
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
    if (obj == null || !(obj instanceof UTF8Statistics)) return false;
    UTF8Statistics that = (UTF8Statistics) obj;
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
    return "UTF8[hasNulls=" + hasNulls + ", min=" + min + ", max=" + max + "]";
  }
}
