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

package com.github.sadikovi.riff.stats

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class UTF8StatisticsSuite extends UnitTestSuite {
  test("init state") {
    val stats = Statistics.sqlTypeToStatistics(StringType)
    stats.getClass should be (classOf[UTF8Statistics])
    stats.hasNulls should be (false)
    stats.getUTF8String(Statistics.ORD_MIN) should be (null)
    stats.getUTF8String(Statistics.ORD_MAX) should be (null)
    stats.isNullAt(Statistics.ORD_MIN) should be (true)
    stats.isNullAt(Statistics.ORD_MAX) should be (true)
  }

  test("update null value") {
    val stats = new UTF8Statistics()
    val row = InternalRow(null, null)
    stats.hasNulls should be (false)
    stats.update(row, 0)
    stats.hasNulls should be (true)
    stats.update(row, 1)
    stats.hasNulls should be (true)
  }

  test("update state") {
    val stats = new UTF8Statistics()
    for (i <- Seq[UTF8String](
        UTF8String.fromString("ddd"),
        UTF8String.fromString("ppp"),
        UTF8String.fromString("aaa"),
        UTF8String.fromString("zzz"))) {
      stats.update(InternalRow(i), 0)
    }
    stats.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("aaa"))
    stats.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("zzz"))
    stats.hasNulls should be (false)
  }

  test("ensure copy when updating state for utf8 stats") {
    val stats = new UTF8Statistics()
    val min = Array[Byte](97, 98, 99, 100)
    val max = Array[Byte](100, 101, 102, 103)
    stats.update(InternalRow(UTF8String.fromBytes(min)), 0)
    stats.update(InternalRow(UTF8String.fromBytes(max)), 0)
    // update min/max arrays
    min(0) = 100
    max(0) = 99
    stats.update(InternalRow(UTF8String.fromBytes(min)), 0)
    stats.update(InternalRow(UTF8String.fromBytes(max)), 0)

    stats.getUTF8String(Statistics.ORD_MIN) should be (
      UTF8String.fromBytes(Array[Byte](97, 98, 99, 100)))
    stats.getUTF8String(Statistics.ORD_MAX) should be (
      UTF8String.fromBytes(Array[Byte](100, 101, 102, 103)))
  }

  test("merge stats") {
    val s1 = new UTF8Statistics()
    s1.update(InternalRow(UTF8String.fromString("aaa")), 0)
    s1.update(InternalRow(UTF8String.fromString("ccc")), 0)
    val s2 = new UTF8Statistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("aaa"))
    s1.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("ddd"))
    s1.hasNulls should be (true)
  }

  test("merge stats 2") {
    val s1 = new UTF8Statistics()
    val s2 = new UTF8Statistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)

    s1.merge(s2)
    s1.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("bbb"))
    s1.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("ddd"))
    s1.hasNulls should be (false)
  }

  test("write/read for empty stats") {
    val buf = new OutputBuffer()
    val stats = new UTF8Statistics()
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("write/read for non-empty stats") {
    val buf = new OutputBuffer()
    val stats = new UTF8Statistics()
    stats.update(InternalRow(UTF8String.fromString("abc")), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(UTF8String.fromString("123")), 0)
    stats.update(InternalRow(UTF8String.fromString("xyz")), 0)
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("toString") {
    val stats = new UTF8Statistics()
    stats.update(InternalRow(UTF8String.fromString("zzz")), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(UTF8String.fromString("aaa")), 0)
    stats.toString should be ("UTF8[hasNulls=true, min=aaa, max=zzz]")
  }
}
