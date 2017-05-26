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
import org.apache.spark.sql.types.ShortType

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class ShortStatisticsSuite extends UnitTestSuite {
  test("init state") {
    val stats = Statistics.sqlTypeToStatistics(ShortType)
    stats.getClass should be (classOf[ShortStatistics])
    stats.hasNulls should be (false)
    stats.getShort(Statistics.ORD_MIN) should be (Short.MaxValue)
    stats.getShort(Statistics.ORD_MAX) should be (Short.MinValue)
    stats.isNullAt(Statistics.ORD_MIN) should be (false)
    stats.isNullAt(Statistics.ORD_MAX) should be (false)
  }

  test("update null value") {
    val stats = new ShortStatistics()
    val row = InternalRow(null, null)
    stats.hasNulls should be (false)
    stats.update(row, 0)
    stats.hasNulls should be (true)
    stats.update(row, 1)
    stats.hasNulls should be (true)
  }

  test("update state") {
    val stats = new ShortStatistics()
    for (i <- Seq[Short](255, -1, 34, -100)) {
      stats.update(InternalRow(i), 0)
    }
    stats.getShort(Statistics.ORD_MIN) should be (-100.toShort)
    stats.getShort(Statistics.ORD_MAX) should be (255.toShort)
    stats.hasNulls should be (false)
  }

  test("merge stats") {
    val s1 = new ShortStatistics()
    s1.update(InternalRow(100.toShort), 0)
    s1.update(InternalRow(400.toShort), 0)
    val s2 = new ShortStatistics()
    s2.update(InternalRow(-100.toShort), 0)
    s2.update(InternalRow(300.toShort), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getShort(Statistics.ORD_MIN) should be (-100.toShort)
    s1.getShort(Statistics.ORD_MAX) should be (400.toShort)
    s1.hasNulls should be (true)
  }

  test("write/read for empty stats") {
    val buf = new OutputBuffer()
    val stats = new ShortStatistics()
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("write/read for non-empty stats") {
    val buf = new OutputBuffer()
    val stats = new ShortStatistics()
    stats.update(InternalRow(123.toShort), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(-123.toShort), 0)
    stats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (stats)
  }

  test("toString") {
    val stats = new ShortStatistics()
    stats.update(InternalRow(43.toShort), 0)
    stats.update(InternalRow(null), 0)
    stats.update(InternalRow(-32.toShort), 0)
    stats.toString should be ("SHORT[hasNulls=true, min=-32, max=43]")
  }
}
