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

class BooleanColumnFilterSuite extends UnitTestSuite {
  test("update null values") {
    val filter = new BooleanColumnFilter()
    filter.hasNulls should be (false)
    filter.update(InternalRow(null), 0)
    filter.hasNulls should be (true)
  }

  test("update non-null values") {
    val filter = new BooleanColumnFilter()
    filter.update(InternalRow(true), 0)
    filter.update(InternalRow(true), 0)
    filter.update(InternalRow(false), 0)

    filter.mightContain(true) should be (true)
    filter.mightContain(false) should be (true)
  }

  test("mightContain") {
    val filter = new BooleanColumnFilter()
    filter.mightContain(false) should be (false)
    filter.mightContain(true) should be (false)

    filter.update(InternalRow(true), 0)
    filter.mightContain(false) should be (false)
    filter.mightContain(true) should be (true)

    filter.update(InternalRow(false), 0)
    filter.mightContain(false) should be (true)
    filter.mightContain(true) should be (true)
  }

  test("equals") {
    val filter = new BooleanColumnFilter()
    assert(filter.equals(filter) === true)
    assert(filter.equals(new BooleanColumnFilter()) === true)
    assert(filter.equals(null) === false)
  }

  test("toString") {
    val filter = new BooleanColumnFilter()
    filter.update(InternalRow(null), 0)
    filter.update(InternalRow(true), 0)
    assert(filter.toString.contains("BooleanColumnFilter[hasNulls=true]"))
  }

  test("read/write empty filter") {
    val buf = new OutputBuffer()
    val filter1 = new BooleanColumnFilter()
    filter1.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    val filter2 = ColumnFilter.readExternal(in)
    filter2 should be (filter1)
    filter2.hasNulls should be (false)
  }

  test("read/write filter") {
    val buf = new OutputBuffer()
    val filter1 = new BooleanColumnFilter()
    filter1.update(InternalRow(null), 0)
    filter1.update(InternalRow(true), 0)
    filter1.update(InternalRow(false), 0)
    filter1.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    val filter2 = ColumnFilter.readExternal(in)
    filter2 should be (filter1)
    filter2.hasNulls should be (true)
    filter2.mightContain(true) should be (true)
    filter2.mightContain(false) should be (true)
  }
}
