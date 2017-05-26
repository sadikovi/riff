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
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Simple boolean statistics as column filter.
 */
class NoopColumnFilter extends ColumnFilter {
  NoopColumnFilter() { }

  @Override
  public boolean mightContain(boolean value) {
    return true;
  }

  @Override
  public boolean mightContain(byte value) {
    return true;
  }

  @Override
  public boolean mightContain(short value) {
    return true;
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
  public void updateNonNullValue(InternalRow row, int ordinal) {
    /* no-op */
  }

  @Override
  protected void writeState(OutputBuffer buffer) throws IOException {
    /* no state to write */
  }

  @Override
  protected void readState(ByteBuffer buffer) throws IOException {
    /* no state to read */
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof NoopColumnFilter;
  }

  @Override
  public String toString() {
    return "NoopColumnFilter[hasNulls=" + hasNulls() + "]";
  }
}
