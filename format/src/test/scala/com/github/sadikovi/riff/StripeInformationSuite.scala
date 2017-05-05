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

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer

import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.riff.io.StripeOutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class StripeInformationSuite extends UnitTestSuite {
  test("init stripe information from stripe output buffer") {
    val buf = new StripeOutputBuffer(123.toByte)
    buf.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8), 0, 8)
    val info = new StripeInformation(buf, 12345L)
    info.id() should be (123)
    info.offset() should be (12345L)
    info.length() should be (8)
    info.hasStatistics() should be (false)
    info.getStatistics() should be (null)
  }

  test("toString method") {
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, null)
    info1.toString should be (s"Stripe[id=123, offset=12345, length=${Int.MaxValue}, " +
      "has_stats=false, has_column_filters=false]")
    val info2 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, Array[Statistics]())
    info2.toString should be (s"Stripe[id=123, offset=12345, length=${Int.MaxValue}, " +
      "has_stats=true, has_column_filters=false]")
    val info3 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, Array[Statistics](),
      Array[ColumnFilter]())
    info3.toString should be (s"Stripe[id=123, offset=12345, length=${Int.MaxValue}, " +
      "has_stats=true, has_column_filters=true]")
  }

  test("assert negative values in stripe information") {
    var err = intercept[IllegalArgumentException] {
      new StripeInformation(-1.toByte, 1L, 1, null)
    }
    err.getMessage should be ("Negative id: -1")

    err = intercept[IllegalArgumentException] {
      new StripeInformation(1.toByte, -1L, 1, null)
    }
    err.getMessage should be ("Negative offset: -1")

    err = intercept[IllegalArgumentException] {
      new StripeInformation(1.toByte, 1L, -1, null)
    }
    err.getMessage should be ("Negative length: -1")
  }

  test("fail magic assertion") {
    val in = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val err = intercept[IOException] {
      StripeInformation.readExternal(in)
    }
    err.getMessage should be (s"Wrong magic: 1 != ${StripeInformation.MAGIC}")
  }

  test("write/read external") {
    val out = new OutputBuffer()
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, null)
    info1.writeExternal(out)

    val in = ByteBuffer.wrap(out.array())
    val info2 = StripeInformation.readExternal(in)

    info2.id() should be (info1.id())
    info2.offset() should be (info1.offset())
    info2.length() should be (info1.length())
    info2.hasStatistics() should be (info1.hasStatistics())
    info2.getStatistics() should be (info1.getStatistics())
    info2.hasColumnFilters() should be (info1.hasColumnFilters())
    info2.getColumnFilters() should be (info1.getColumnFilters())
    info2.toString() should be (info1.toString())
  }

  test("assert null statistics") {
    val out = new OutputBuffer()
    val stats = Array[Statistics](null)
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, stats)
    val err = intercept[NullPointerException] {
      info1.writeExternal(out)
    }
    err.getMessage should be (s"Encountered null statistics for stripe $info1")
  }

  test("write/read external with statistics") {
    val out = new OutputBuffer()
    val stats = Array(
      Statistics.sqlTypeToStatistics(IntegerType),
      Statistics.sqlTypeToStatistics(LongType),
      Statistics.sqlTypeToStatistics(StringType))
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, stats)
    info1.writeExternal(out)

    val in = ByteBuffer.wrap(out.array())
    val info2 = StripeInformation.readExternal(in)

    info2.id() should be (info1.id())
    info2.offset() should be (info1.offset())
    info2.length() should be (info1.length())
    info2.hasStatistics() should be (info1.hasStatistics())
    info2.getStatistics() should be (info1.getStatistics())
    info2.toString() should be (info1.toString())
  }

  test("equality") {
    var stripe1 = new StripeInformation(1.toByte, 123L, 100, null)
    var stripe2 = new StripeInformation(1.toByte, 123L, 100, null)
    assert(stripe1 == stripe2)
    assert(stripe1.equals(stripe1))
    assert(stripe2.equals(stripe2))

    assert(!stripe1.equals(null))

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null)
    stripe2 = new StripeInformation(2.toByte, 123L, 100, null)
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null)
    stripe2 = new StripeInformation(1.toByte, 124L, 100, null)
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null)
    stripe2 = new StripeInformation(1.toByte, 123L, 101, null)
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, Array.empty[Statistics])
    stripe2 = new StripeInformation(1.toByte, 123L, 100, null)
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100,
      Array(Statistics.sqlTypeToStatistics(IntegerType)))
    stripe2 = new StripeInformation(1.toByte, 123L, 100,
      Array(Statistics.sqlTypeToStatistics(LongType)))
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100,
      Array(Statistics.sqlTypeToStatistics(IntegerType)))
    stripe2 = new StripeInformation(1.toByte, 123L, 100,
      Array(Statistics.sqlTypeToStatistics(IntegerType)))
    assert(stripe1 == stripe2)
  }

  test("assert null column filter") {
    val out = new OutputBuffer()
    val filters = Array[ColumnFilter](null)
    val info = new StripeInformation(123.toByte, 12345L, Int.MaxValue, null, filters)
    val err = intercept[NullPointerException] {
      info.writeExternal(out)
    }
    err.getMessage should be (s"Encountered null column filter for stripe $info")
  }

  test("write/read external with column filters") {
    val out = new OutputBuffer()
    val filters = Array(
      ColumnFilter.sqlTypeToColumnFilter(IntegerType, 10),
      ColumnFilter.sqlTypeToColumnFilter(LongType, 10),
      ColumnFilter.sqlTypeToColumnFilter(StringType, 10))
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue, null, filters)
    info1.writeExternal(out)

    val in = ByteBuffer.wrap(out.array())
    val info2 = StripeInformation.readExternal(in)

    info2.id() should be (info1.id())
    info2.offset() should be (info1.offset())
    info2.length() should be (info1.length())
    info2.hasStatistics() should be (info1.hasStatistics())
    info2.getStatistics() should be (info1.getStatistics())
    info2.hasColumnFilters() should be (info1.hasColumnFilters())
    info2.getColumnFilters() should be (info1.getColumnFilters())
    info2.toString() should be (info1.toString())
  }

  test("equality with column filters") {
    var stripe1 = new StripeInformation(1.toByte, 123L, 100, null,
      Array[ColumnFilter](null))
    var stripe2 = new StripeInformation(1.toByte, 123L, 100, null)
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.noopFilter))
    stripe2 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.noopFilter))
    assert(stripe1 == stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.sqlTypeToColumnFilter(IntegerType, 10)))
    stripe2 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.noopFilter))
    assert(stripe1 != stripe2)

    stripe1 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.sqlTypeToColumnFilter(IntegerType, 10)))
    stripe2 = new StripeInformation(1.toByte, 123L, 100, null,
      Array(ColumnFilter.sqlTypeToColumnFilter(IntegerType, 20)))
    assert(stripe1 == stripe2)
  }
}
