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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

class UTF8ColumnFilter extends BloomColumnFilter {
  UTF8ColumnFilter() {
    super();
  }

  UTF8ColumnFilter(long numItems) {
    super(numItems);
  }

  @Override
  public boolean mightContain(UTF8String value) {
    if (value == null) return hasNulls();
    return filter.mightContainBinary(value.getBytes());
  }

  @Override
  public void updateNonNullValue(InternalRow row, int ordinal) {
    filter.putBinary(row.getUTF8String(ordinal).getBytes());
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof UTF8ColumnFilter;
  }

  @Override
  public String toString() {
    return "UTF8ColumnFilter[hasNulls=" + hasNulls() + ", " + filter + "]";
  }
}
