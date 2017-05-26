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

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Byte column filter backed by bit set.
 */
class ByteColumnFilter extends ColumnFilter {
  // size of the bit set
  private static final int BYTES = 32;

  private byte[] set;

  ByteColumnFilter() {
    // we need 256 bits in total to map all possible values
    this.set = new byte[BYTES];
  }

  private boolean getBit(byte value) {
    int i = value - Byte.MIN_VALUE;
    return (set[i / 8] & (1 << (i % 8))) != 0;
  }

  private void setBit(byte value) {
    int i = value - Byte.MIN_VALUE;
    set[i / 8] |= 1 << (i % 8);
  }

  @Override
  public void updateNonNullValue(InternalRow row, int ordinal) {
    setBit(row.getByte(ordinal));
  }

  @Override
  public boolean mightContain(byte value) {
    return getBit(value);
  }

  @Override
  protected void writeState(OutputBuffer buffer) throws IOException {
    buffer.writeBytes(set);
  }

  @Override
  protected void readState(ByteBuffer buffer) throws IOException {
    set = new byte[BYTES];
    buffer.get(set);
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof ByteColumnFilter;
  }

  @Override
  public String toString() {
    return "ByteColumnFilter[hasNulls=" + hasNulls() + ", size=" + BYTES + "]";
  }
}
