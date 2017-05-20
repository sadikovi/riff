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

package com.github.sadikovi.riff

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class ColumnFilterSuite extends UnitTestSuite {
  test("noop filter returns true for all contains methods") {
    val filter = ColumnFilter.noopFilter()
    filter.mightContain(1) should be (true)
    filter.mightContain(1L) should be (true)
    filter.mightContain(UTF8String.fromString("1")) should be (true)
  }

  test("noop filter does not write state except id") {
    val filter = ColumnFilter.noopFilter()
    val out = new OutputBuffer()
    filter.writeExternal(out)
    out.array should be (Array[Byte](1))
  }

  test("noop filter write/read") {
    val filter1 = ColumnFilter.noopFilter()
    val out = new OutputBuffer()
    filter1.writeExternal(out)

    val filter2 = ColumnFilter.readExternal(ByteBuffer.wrap(out.array));
    filter2 should be (filter1)
  }

  test("select bloom filter for different types") {
    val row = InternalRow(1, 2L, UTF8String.fromString("3"), 5.toShort, 6.toByte)
    val filter1 = ColumnFilter.sqlTypeToColumnFilter(IntegerType, 64)
    filter1.mightContain(row.getInt(0)) should be (false)
    filter1.update(row, 0)
    filter1.mightContain(row.getInt(0)) should be (true)

    val filter2 = ColumnFilter.sqlTypeToColumnFilter(LongType, 64)
    filter2.mightContain(row.getLong(1)) should be (false)
    filter2.update(row, 1)
    filter2.mightContain(row.getLong(1)) should be (true)

    val filter3 = ColumnFilter.sqlTypeToColumnFilter(StringType, 64)
    filter3.mightContain(row.getUTF8String(2)) should be (false)
    filter3.update(row, 2)
    filter3.mightContain(row.getUTF8String(2)) should be (true)

    // DateType is backed by integer type in Spark SQL
    val filter4 = ColumnFilter.sqlTypeToColumnFilter(DateType, 64)
    filter4.mightContain(row.getInt(0)) should be (false)
    filter4.update(row, 0)
    filter4.mightContain(row.getInt(0)) should be (true)

    // TimestampType is backed by long type in Spark SQL
    val filter5 = ColumnFilter.sqlTypeToColumnFilter(TimestampType, 64)
    filter5.mightContain(row.getLong(1)) should be (false)
    filter5.update(row, 1)
    filter5.mightContain(row.getLong(1)) should be (true)

    val filter6 = ColumnFilter.sqlTypeToColumnFilter(ShortType, 64)
    filter6.mightContain(row.getShort(3)) should be (false)
    filter6.update(row, 3)
    filter6.mightContain(row.getShort(3)) should be (true)

    val filter7 = ColumnFilter.sqlTypeToColumnFilter(ByteType, 64)
    filter7.mightContain(row.getByte(4)) should be (false)
    filter7.update(row, 4)
    filter7.mightContain(row.getByte(4)) should be (true)
  }

  test("bloom filter write/read") {
    val row = InternalRow(1, 2L, UTF8String.fromString("3"))
    val out = new OutputBuffer()
    val filter1 = ColumnFilter.sqlTypeToColumnFilter(IntegerType, 64)
    filter1.update(row, 0)
    filter1.writeExternal(out)
    val filter2 = ColumnFilter.readExternal(ByteBuffer.wrap(out.array))
    filter2 should be (filter1)
    filter2.mightContain(row.getInt(0)) should be (true)
  }

  test("boolean filter - update/mightContain") {
    val filter = ColumnFilter.sqlTypeToColumnFilter(BooleanType, 0)
    filter.mightContain(false) should be (false)
    filter.mightContain(true) should be (false)
    // update true values only
    filter.update(InternalRow(true), 0)
    filter.mightContain(false) should be (false)
    filter.mightContain(true) should be (true)
    // filter should contain both values
    filter.update(InternalRow(false), 0)
    filter.mightContain(false) should be (true)
    filter.mightContain(true) should be (true)
  }

  test("boolean filter write/read") {
    val row = InternalRow(true)
    val out = new OutputBuffer()
    val filter1 = ColumnFilter.sqlTypeToColumnFilter(BooleanType, 0)
    filter1.update(row, 0)
    filter1.writeExternal(out)
    val filter2 = ColumnFilter.readExternal(ByteBuffer.wrap(out.array))
    filter2 should be (filter1)
    filter2.mightContain(true) should be (true)
    filter2.mightContain(false) should be (false)
  }

  test("select noop filter for unsupported data type") {
    val filter = ColumnFilter.sqlTypeToColumnFilter(NullType, 10)
    filter should be (ColumnFilter.noopFilter)
  }

  test("fail to read column filter with invalid id") {
    val err = intercept[IOException] {
      ColumnFilter.readExternal(ByteBuffer.wrap(Array[Byte](Byte.MinValue)))
    }
    err.getMessage should be (s"Unknown column filter id: ${Byte.MinValue}")
  }
}
