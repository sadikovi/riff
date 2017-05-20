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
        override def merge(obj: Statistics): Unit = ???
      }
    }
    err.getMessage should be ("Negative id: -1");
  }

  test("init state") {
    val intStats = Statistics.sqlTypeToStatistics(IntegerType)
    intStats.getClass should be (classOf[IntStatistics])
    intStats.id should be (IntStatistics.ID)
    intStats.hasNulls should be (false)

    val dateStats = Statistics.sqlTypeToStatistics(DateType)
    dateStats.getClass should be (classOf[IntStatistics])
    dateStats.id should be (IntStatistics.ID)
    dateStats.hasNulls should be (false)

    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.getClass should be (classOf[LongStatistics])
    longStats.id should be (LongStatistics.ID)
    longStats.hasNulls should be (false)

    val timestampStats = Statistics.sqlTypeToStatistics(TimestampType)
    timestampStats.getClass should be (classOf[LongStatistics])
    timestampStats.id should be (LongStatistics.ID)
    timestampStats.hasNulls should be (false)

    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.getClass should be (classOf[UTF8StringStatistics])
    utfStats.id should be (UTF8StringStatistics.ID)
    utfStats.hasNulls should be (false)

    val booleanStats = Statistics.sqlTypeToStatistics(BooleanType)
    booleanStats.getClass should be (classOf[BooleanStatistics])
    booleanStats.id should be (BooleanStatistics.ID)
    booleanStats.hasNulls should be (false)

    val shortStats = Statistics.sqlTypeToStatistics(ShortType)
    shortStats.getClass should be (classOf[ShortStatistics])
    shortStats.id should be (ShortStatistics.ID)
    shortStats.hasNulls should be (false)

    val byteStats = Statistics.sqlTypeToStatistics(ByteType)
    byteStats.getClass should be (classOf[ByteStatistics])
    byteStats.id should be (ByteStatistics.ID)
    byteStats.hasNulls should be (false)

    // null type is not supported
    val noopStats = Statistics.sqlTypeToStatistics(NullType)
    noopStats.getClass should be (classOf[NoopStatistics])
    noopStats.id should be (NoopStatistics.ID)
    noopStats.hasNulls should be (false)
  }

  test("update null value") {
    val stats = new Statistics(127) {
      override def updateState(row: InternalRow, ordinal: Int): Unit = ???
      override def writeState(buf: OutputBuffer): Unit = ???
      override def readState(buf: ByteBuffer): Unit = ???
      override def merge(obj: Statistics): Unit = ???
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
    intStats.getInt(Statistics.ORD_MIN) should be (Int.MaxValue)
    intStats.getInt(Statistics.ORD_MAX) should be (Int.MinValue)

    intStats.update(InternalRow(255), 0)
    intStats.getInt(Statistics.ORD_MIN) should be (255)
    intStats.getInt(Statistics.ORD_MAX) should be (255)

    intStats.update(InternalRow(-255), 0)
    intStats.getInt(Statistics.ORD_MIN) should be (-255)
    intStats.getInt(Statistics.ORD_MAX) should be (255)
  }

  test("update state for long stats") {
    val longStats = Statistics.sqlTypeToStatistics(LongType)
    longStats.getLong(Statistics.ORD_MIN) should be (Long.MaxValue)
    longStats.getLong(Statistics.ORD_MAX) should be (Long.MinValue)

    longStats.update(InternalRow(12345L), 0)
    longStats.getLong(Statistics.ORD_MIN) should be (12345L)
    longStats.getLong(Statistics.ORD_MAX) should be (12345L)

    longStats.update(InternalRow(-12345L), 0)
    longStats.getLong(Statistics.ORD_MIN) should be (-12345L)
    longStats.getLong(Statistics.ORD_MAX) should be (12345L)
  }

  test("update state for utf8 stats") {
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    utfStats.isNullAt(Statistics.ORD_MIN) should be (true)
    utfStats.isNullAt(Statistics.ORD_MAX) should be (true)
    utfStats.getUTF8String(Statistics.ORD_MIN) should be (null)
    utfStats.getUTF8String(Statistics.ORD_MAX) should be (null)

    utfStats.update(InternalRow(UTF8String.fromString("abc")), 0)
    utfStats.isNullAt(Statistics.ORD_MIN) should be (false)
    utfStats.isNullAt(Statistics.ORD_MAX) should be (false)
    utfStats.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("abc"))
    utfStats.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("abc"))

    utfStats.update(InternalRow(UTF8String.fromString("123")), 0)
    utfStats.isNullAt(Statistics.ORD_MIN) should be (false)
    utfStats.isNullAt(Statistics.ORD_MAX) should be (false)
    utfStats.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("123"))
    utfStats.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("abc"))
  }

  test("update state for boolean stats") {
    val booleanStats = Statistics.sqlTypeToStatistics(BooleanType)
    booleanStats.isNullAt(Statistics.ORD_MIN) should be (true)
    booleanStats.isNullAt(Statistics.ORD_MAX) should be (true)
    intercept[IllegalStateException] { booleanStats.getBoolean(Statistics.ORD_MIN) }
    intercept[IllegalStateException] { booleanStats.getBoolean(Statistics.ORD_MAX) }

    booleanStats.update(InternalRow(false), 0)
    booleanStats.isNullAt(Statistics.ORD_MIN) should be (false)
    booleanStats.isNullAt(Statistics.ORD_MAX) should be (false)
    booleanStats.getBoolean(Statistics.ORD_MIN) should be (false)
    booleanStats.getBoolean(Statistics.ORD_MAX) should be (false)

    booleanStats.update(InternalRow(true), 0)
    booleanStats.isNullAt(Statistics.ORD_MIN) should be (false)
    booleanStats.isNullAt(Statistics.ORD_MAX) should be (false)
    booleanStats.getBoolean(Statistics.ORD_MIN) should be (false)
    booleanStats.getBoolean(Statistics.ORD_MAX) should be (true)
  }

  test("update state for short stats") {
    val shortStats = Statistics.sqlTypeToStatistics(ShortType)
    shortStats.getShort(Statistics.ORD_MIN) should be (Short.MaxValue)
    shortStats.getShort(Statistics.ORD_MAX) should be (Short.MinValue)

    shortStats.update(InternalRow(51.toShort), 0)
    shortStats.getShort(Statistics.ORD_MIN) should be (51)
    shortStats.getShort(Statistics.ORD_MAX) should be (51)

    shortStats.update(InternalRow(-67.toShort), 0)
    shortStats.getShort(Statistics.ORD_MIN) should be (-67)
    shortStats.getShort(Statistics.ORD_MAX) should be (51)
  }

  test("update state for byte stats") {
    val byteStats = Statistics.sqlTypeToStatistics(ByteType)
    byteStats.getByte(Statistics.ORD_MIN) should be (Byte.MaxValue)
    byteStats.getByte(Statistics.ORD_MAX) should be (Byte.MinValue)

    byteStats.update(InternalRow(51.toByte), 0)
    byteStats.getByte(Statistics.ORD_MIN) should be (51)
    byteStats.getByte(Statistics.ORD_MAX) should be (51)

    byteStats.update(InternalRow(-67.toByte), 0)
    byteStats.getByte(Statistics.ORD_MIN) should be (-67)
    byteStats.getByte(Statistics.ORD_MAX) should be (51)
  }

  test("ensure copy when updating state for utf8 stats") {
    val utfStats = Statistics.sqlTypeToStatistics(StringType)
    val min = Array[Byte](97, 98, 99, 100)
    val max = Array[Byte](100, 101, 102, 103)
    utfStats.update(InternalRow(UTF8String.fromBytes(min)), 0)
    utfStats.update(InternalRow(UTF8String.fromBytes(max)), 0)
    // update min/max arrays
    min(0) = 100
    max(0) = 99
    utfStats.update(InternalRow(UTF8String.fromBytes(min)), 0)
    utfStats.update(InternalRow(UTF8String.fromBytes(max)), 0)

    utfStats.getUTF8String(Statistics.ORD_MIN) should be (
      UTF8String.fromBytes(Array[Byte](97, 98, 99, 100)))
    utfStats.getUTF8String(Statistics.ORD_MAX) should be (
      UTF8String.fromBytes(Array[Byte](100, 101, 102, 103)))
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

  test("write/read for empty boolean stats") {
    val buf = new OutputBuffer()
    val booleanStats = Statistics.sqlTypeToStatistics(BooleanType)
    booleanStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (booleanStats)
  }

  test("write/read for non-empty boolean stats") {
    val buf = new OutputBuffer()
    val booleanStats = Statistics.sqlTypeToStatistics(BooleanType)
    booleanStats.update(InternalRow(true), 0)
    booleanStats.update(InternalRow(null), 0)
    booleanStats.update(InternalRow(false), 0)
    booleanStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (booleanStats)
  }

  test("write/read for empty short stats") {
    val buf = new OutputBuffer()
    val shortStats = Statistics.sqlTypeToStatistics(ShortType)
    shortStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (shortStats)
  }

  test("write/read for non-empty short stats") {
    val buf = new OutputBuffer()
    val shortStats = Statistics.sqlTypeToStatistics(ShortType)
    shortStats.update(InternalRow(51.toShort), 0)
    shortStats.update(InternalRow(null), 0)
    shortStats.update(InternalRow(-67.toShort), 0)
    shortStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (shortStats)
  }

  test("write/read for empty byte stats") {
    val buf = new OutputBuffer()
    val byteStats = Statistics.sqlTypeToStatistics(ByteType)
    byteStats.writeExternal(buf)
    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (byteStats)
  }

  test("write/read for non-empty byte stats") {
    val buf = new OutputBuffer()
    val byteStats = Statistics.sqlTypeToStatistics(ByteType)
    byteStats.update(InternalRow(51.toByte), 0)
    byteStats.update(InternalRow(null), 0)
    byteStats.update(InternalRow(-67.toByte), 0)
    byteStats.writeExternal(buf)

    val in = ByteBuffer.wrap(buf.array())
    Statistics.readExternal(in) should be (byteStats)
  }

  test("merge int stats") {
    val s1 = new IntStatistics()
    s1.update(InternalRow(100), 0)
    s1.update(InternalRow(400), 0)
    val s2 = new IntStatistics()
    s2.update(InternalRow(-100), 0)
    s2.update(InternalRow(300), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getInt(Statistics.ORD_MIN) should be (-100)
    s1.getInt(Statistics.ORD_MAX) should be (400)
    s1.hasNulls should be (true)
  }

  test("merge long stats") {
    val s1 = new LongStatistics()
    s1.update(InternalRow(100L), 0)
    s1.update(InternalRow(700L), 0)
    val s2 = new LongStatistics()
    s2.update(InternalRow(-100L), 0)
    s2.update(InternalRow(500L), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getLong(Statistics.ORD_MIN) should be (-100L)
    s1.getLong(Statistics.ORD_MAX) should be (700L)
    s1.hasNulls should be (true)
  }

  test("merge utf8 stats 1") {
    val s1 = new UTF8StringStatistics()
    s1.update(InternalRow(UTF8String.fromString("aaa")), 0)
    s1.update(InternalRow(UTF8String.fromString("ccc")), 0)
    val s2 = new UTF8StringStatistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("aaa"))
    s1.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("ddd"))
    s1.hasNulls should be (true)
  }

  test("merge utf8 stats 2") {
    val s1 = new UTF8StringStatistics()
    val s2 = new UTF8StringStatistics()
    s2.update(InternalRow(UTF8String.fromString("bbb")), 0)
    s2.update(InternalRow(UTF8String.fromString("ddd")), 0)

    s1.merge(s2)
    s1.getUTF8String(Statistics.ORD_MIN) should be (UTF8String.fromString("bbb"))
    s1.getUTF8String(Statistics.ORD_MAX) should be (UTF8String.fromString("ddd"))
    s1.hasNulls should be (false)
  }

  test("merge boolean stats 1") {
    val s1 = new BooleanStatistics()
    s1.update(InternalRow(false), 0)
    val s2 = new BooleanStatistics()
    s2.update(InternalRow(true), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.isNullAt(Statistics.ORD_MIN) should be (false)
    s1.isNullAt(Statistics.ORD_MAX) should be (false)
    s1.getBoolean(Statistics.ORD_MIN) should be (false)
    s1.getBoolean(Statistics.ORD_MAX) should be (true)
    s1.hasNulls should be (true)
  }

  test("merge boolean stats 2") {
    val s1 = new BooleanStatistics()
    val s2 = new BooleanStatistics()
    s2.update(InternalRow(true), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    s1.isNullAt(Statistics.ORD_MIN) should be (false)
    s1.isNullAt(Statistics.ORD_MAX) should be (false)
    s1.getBoolean(Statistics.ORD_MIN) should be (true)
    s1.getBoolean(Statistics.ORD_MAX) should be (true)
    s1.hasNulls should be (true)
  }

  test("merge boolean stats 3") {
    val s1 = new BooleanStatistics()
    val s2 = new BooleanStatistics()

    s1.merge(s2)
    s1.isNullAt(Statistics.ORD_MIN) should be (true)
    s1.isNullAt(Statistics.ORD_MAX) should be (true)
    s1.hasNulls should be (false)
  }

  test("merge short stats") {
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

  test("merge byte stats") {
    val s1 = new ByteStatistics()
    s1.update(InternalRow(10.toByte), 0)
    s1.update(InternalRow(40.toByte), 0)
    val s2 = new ByteStatistics()
    s2.update(InternalRow(-10.toByte), 0)
    s2.update(InternalRow(30.toByte), 0)
    s2.update(InternalRow(null), 0)

    s1.merge(s2)
    assert(s1 != s2)
    s1.getByte(Statistics.ORD_MIN) should be (-10.toByte)
    s1.getByte(Statistics.ORD_MAX) should be (40.toByte)
    s1.hasNulls should be (true)
  }
}
