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

package com.github.sadikovi.riff.column

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class LongColumnFilterSuite extends UnitTestSuite {
  test("update null values") {
    val filter = new LongColumnFilter()
    filter.hasNulls should be (false)
    filter.update(InternalRow(null), 0)
    filter.hasNulls should be (true)
  }

  test("update non-null values") {
    val filter = new LongColumnFilter()
    filter.update(InternalRow(1L), 0)
    filter.update(InternalRow(2L), 0)
    filter.update(InternalRow(10L), 0)

    filter.mightContain(1L) should be (true)
    filter.mightContain(2L) should be (true)
    filter.mightContain(10L) should be (true)
    filter.mightContain(-1L) should be (false)
    filter.mightContain(3L) should be (false)
    filter.mightContain(11L) should be (false)
  }

  test("equals") {
    val filter = new LongColumnFilter()
    assert(filter.equals(filter) === true)
    assert(filter.equals(new LongColumnFilter(1024)) === true)
    assert(filter.equals(null) === false)
  }

  test("toString") {
    val filter = new LongColumnFilter(1024)
    filter.update(InternalRow(null), 0)
    filter.update(InternalRow(1L), 0)
    assert(filter.toString
      .contains("LongColumnFilter[hasNulls=true, org.apache.spark.util.sketch.BloomFilterImpl"))
  }

  test("read/write empty filter") {
    val buf = new OutputBuffer()
    val filter1 = new LongColumnFilter(32)
    filter1.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    val filter2 = ColumnFilter.readExternal(in)
    filter2 should be (filter1)
    filter2.hasNulls should be (false)
  }

  test("read/write filter") {
    val buf = new OutputBuffer()
    val filter1 = new LongColumnFilter(32)
    filter1.update(InternalRow(null), 0)
    filter1.update(InternalRow(1L), 0)
    filter1.update(InternalRow(2L), 0)
    filter1.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    val filter2 = ColumnFilter.readExternal(in)
    filter2 should be (filter1)
    filter2.hasNulls should be (true)
    filter2.mightContain(1L) should be (true)
    filter2.mightContain(2L) should be (true)
  }
}
