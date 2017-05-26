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
 * Simple boolean statistics as column filter.
 */
class BooleanColumnFilter extends ColumnFilter {
  // whether or not filter contains "true"
  protected boolean containsTrue;
  // whether or not filter contains "false"
  protected boolean containsFalse;

  BooleanColumnFilter() {
    containsTrue = false;
    containsFalse = false;
  }

  @Override
  public void updateNonNullValue(InternalRow row, int ordinal) {
    boolean value = row.getBoolean(ordinal);
    if (value) {
      containsTrue = true;
    } else {
      containsFalse = true;
    }
  }

  @Override
  public boolean mightContain(boolean value) {
    return value ? containsTrue : containsFalse;
  }

  @Override
  protected void writeState(OutputBuffer buffer) throws IOException {
    int flags = 0;
    flags |= containsTrue ? 1 : 0;
    flags |= containsFalse ? 2 : 0;
    buffer.writeByte(flags);
  }

  @Override
  protected void readState(ByteBuffer buffer) throws IOException {
    byte flags = buffer.get();
    containsTrue = (flags & 1) != 0;
    containsFalse = (flags & 2) != 0;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof BooleanColumnFilter;
  }

  @Override
  public String toString() {
    return "BooleanColumnFilter[hasNulls=" + hasNulls() + "]";
  }
}
