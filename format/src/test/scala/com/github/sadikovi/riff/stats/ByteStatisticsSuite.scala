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
import org.apache.spark.sql.types.ByteType

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class ByteStatisticsSuite extends UnitTestSuite {
  test("init state") {
    val stats = Statistics.sqlTypeToStatistics(ByteType)
    stats.getClass should be (classOf[ByteStatistics])
    stats.hasNulls should be (false)
    stats.getByte(Statistics.ORD_MIN) should be (Byte.MaxValue)
    stats.getByte(Statistics.ORD_MAX) should be (Byte.MinValue)
    stats.isNullAt(Statistics.ORD_MIN) should be (false)
    stats.isNullAt(Statistics.ORD_MAX) should be (false)
  }

  test("update null value") {
    val stats = new ByteStatistics()
    val row = InternalRow(null, null)
    stats.hasNulls should be (false)
    stats.update(row, 0)
    stats.hasNulls should be (true)
    stats.update(row, 1)
    stats.hasNulls should be (true)
  }

  test("update state") {
    val stats = new ByteStatistics()
    for (i <- Seq[Byte](127, -1, 34, -100)) {
      stats.update(InternalRow(i), 0)
    }
    stats.getByte(Statistics.ORD_MIN) should be (-100.toByte)
    stats.getByte(Statistics.ORD_MAX) should be (127.toByte)
    stats.hasNulls should be (false)
  }

  test("merge stats") {
    val s1 = new ByteStatistics()
    s1.update(InternalRow(100.toByte), 0)
    s1.update(InternalRow(120.toByte), 0)
    val s2 = new ByteStatistics()
    s2.update(InternalRow(-100.toByte), 0)
    s2.update(InternalRow(80.toByte), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getByte(Statistics.ORD_MIN) should be (-100.toByte)
    s1.getByte(Statistics.ORD_MAX) should be (120.toByte)
    s1.hasNulls should be (true)
  }

  test("write/read for empty stats") {
    val buf = new OutputBuffer()
    val stats = new ByteStatistics()
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("write/read for non-empty stats") {
    val buf = new OutputBuffer()
    val stats = new ByteStatistics()
    stats.update(InternalRow(123.toByte), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(-123.toByte), 0)
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("toString") {
    val stats = new ByteStatistics()
    stats.update(InternalRow(43.toByte), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(-32.toByte), 0)
    stats.toString should be ("BYTE[hasNulls=true, min=-32, max=43]")
  }
}
