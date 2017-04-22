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

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class StatisticsSuite extends UnitTestSuite {
  import Statistics._

  test("id should be positive") {
    val err = intercept[IllegalArgumentException] {
      new Statistics(-1) {
        override def updateState(row: InternalRow, ordinal: Int): Unit = ???
        override def writeState(buf: OutputBuffer): Unit = ???
        override def readState(buf: ByteBuffer): Unit = ???
        override def combine(obj: Statistics): Statistics = ???
      }
    }
    err.getMessage should be ("Negative id: -1");
  }

  test("init state") {
    val intStats = Statistics.sqlTypeToStatistics(IntegerType)
    intStats.getClass should be (classOf[IntStatistics])
    intStats.id should be (IntStatistics.ID)
    intStats.hasNulls should be (false)

    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.getClass should be (classOf[LongStatistics])
    longStats.id should be (LongStatistics.ID)
    longStats.hasNulls should be (false)

    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.getClass should be (classOf[UTF8StringStatistics])
    utfStats.id should be (UTF8StringStatistics.ID)
    utfStats.hasNulls should be (false)

    // date type is not supported
    val noopStats = Statistics.sqlTypeToStatistics(DateType)
    noopStats.getClass should be (classOf[NoopStatistics])
    noopStats.id should be (NoopStatistics.ID)
    noopStats.hasNulls should be (false)
  }

  test("update null value") {
    val stats = new Statistics(127) {
      override def updateState(row: InternalRow, ordinal: Int): Unit = ???
      override def writeState(buf: OutputBuffer): Unit = ???
      override def readState(buf: ByteBuffer): Unit = ???
      override def combine(obj: Statistics): Statistics = ???
    }
    stats.hasNulls should be (false)

    val row = InternalRow(null, null)
    stats.update(row, 0)
    stats.hasNulls should be (true)
    stats.update(row, 1)
    stats.hasNulls should be (true)
  }

  test("update state for int stats") {
    val intStats = Statistics.sqlTypeToStatistics(IntegerType)
    intStats.getMin should be (Int.MaxValue)
    intStats.getMax should be (Int.MinValue)

    intStats.update(InternalRow(255), 0)
    intStats.getMin should be (255)
    intStats.getMax should be (255)

    intStats.update(InternalRow(-255), 0)
    intStats.getMin should be (-255)
    intStats.getMax should be (255)
  }

  test("update state for long stats") {
    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.getMin should be (Long.MaxValue)
    longStats.getMax should be (Long.MinValue)

    longStats.update(InternalRow(12345L), 0)
    longStats.getMin should be (12345L)
    longStats.getMax should be (12345L)

    longStats.update(InternalRow(-12345L), 0)
    longStats.getMin should be (-12345L)
    longStats.getMax should be (12345L)
  }

  test("update state for utf8 stats") {
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.getMin should be (null)
    utfStats.getMax should be (null)

    utfStats.update(InternalRow(UTF8String.fromString("abc")), 0)
    utfStats.getMin should be (UTF8String.fromString("abc"))
    utfStats.getMax should be (UTF8String.fromString("abc"))

    utfStats.update(InternalRow(UTF8String.fromString("123")), 0)
    utfStats.getMin should be (UTF8String.fromString("123"))
    utfStats.getMax should be (UTF8String.fromString("abc"))
  }

  test("write/read for empty int stats") {
    val buf = new OutputBuffer()
    val intStats = Statistics.sqlTypeToStatistics(IntegerType)
    intStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (intStats)
  }

  test("write/read for non-empty int stats") {
    val buf = new OutputBuffer()
    val intStats = Statistics.sqlTypeToStatistics(IntegerType)
    intStats.update(InternalRow(123), 0)
    intStats.update(InternalRow(null), 0)
    intStats.update(InternalRow(-123), 0)
    intStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (intStats)
  }

  test("write/read for empty long stats") {
    val buf = new OutputBuffer()
    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (longStats)
  }

  test("write/read for non-empty long stats") {
    val buf = new OutputBuffer()
    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.update(InternalRow(123L), 0)
    longStats.update(InternalRow(null), 0)
    longStats.update(InternalRow(-123L), 0)
    longStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (longStats)
  }

  test("write/read for empty utf stats") {
    val buf = new OutputBuffer()
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (utfStats)
  }

  test("write/read for non-empty null utf stats") {
    val buf = new OutputBuffer()
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.update(InternalRow(UTF8String.fromString("abc")), 0)
    utfStats.update(InternalRow(null), 0)
    utfStats.update(InternalRow(UTF8String.fromString("123")), 0)
    utfStats.update(InternalRow(UTF8String.fromString("xyz")), 0)
    utfStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (utfStats)
  }

  test("write/read for non-empty non-null utf stats") {
    val buf = new OutputBuffer()
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.update(InternalRow(UTF8String.fromString("abc")), 0)
    utfStats.update(InternalRow(UTF8String.fromString("123")), 0)
    utfStats.update(InternalRow(UTF8String.fromString("xyz")), 0)
    utfStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (utfStats)
  }

  test("merge int stats") {
    val s1 = new IntStatistics()
    s1.update(InternalRow(100), 0)
    s1.update(InternalRow(400), 0)
    val s2 = new IntStatistics()
    s2.update(InternalRow(-100), 0)
    s2.update(InternalRow(300), 0)
    s2.update(InternalRow(null), 0)

    val s3 = s1.combine(s2)
    assert(s3 != s1)
    assert(s3 != s2)
    s3.getMin should be (-100)
    s3.getMax should be (400)
    s3.hasNulls should be (true)
  }

  test("merge long stats") {
    val s1 = new LongStatistics()
    s1.update(InternalRow(100L), 0)
    s1.update(InternalRow(700L), 0)
    val s2 = new LongStatistics()
    s2.update(InternalRow(-100L), 0)
    s2.update(InternalRow(500L), 0)
    s2.update(InternalRow(null), 0)

    val s3 = s1.combine(s2)
    assert(s3 != s1)
    assert(s3 != s2)
    s3.getMin should be (-100L)
    s3.getMax should be (700L)
    s3.hasNulls should be (true)
  }

  test("merge utf8 stats 1") {
    val s1 = new UTF8StringStatistics()
    s1.update(InternalRow(UTF8String.fromString("aaa")), 0)
    s1.update(InternalRow(UTF8String.fromString("ccc")), 0)
    val s2 = new UTF8StringStatistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)
    s2.update(InternalRow(null), 0)

    val s3 = s1.combine(s2)
    assert(s3 != s1)
    assert(s3 != s2)
    s3.getMin should be (UTF8String.fromString("aaa"))
    s3.getMax should be (UTF8String.fromString("ddd"))
    s3.hasNulls should be (true)
  }

  test("merge utf8 stats 2") {
    val s1 = new UTF8StringStatistics()
    val s2 = new UTF8StringStatistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)

    val s3 = s1.combine(s2)
    s3.getMin should be (UTF8String.fromString("bbb"))
    s3.getMax should be (UTF8String.fromString("ddd"))
    s3.hasNulls should be (false)
  }
}
