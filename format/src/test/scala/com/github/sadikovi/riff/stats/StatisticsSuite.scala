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
import org.apache.spark.sql.types._

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class StatisticsSuite extends UnitTestSuite {
  test("select statistics for sql type") {
    Statistics.sqlTypeToStatistics(IntegerType) should be (new IntStatistics())
    Statistics.sqlTypeToStatistics(LongType) should be (new LongStatistics())
    Statistics.sqlTypeToStatistics(StringType) should be (new UTF8Statistics())
    Statistics.sqlTypeToStatistics(DateType) should be (new DateStatistics())
    Statistics.sqlTypeToStatistics(TimestampType) should be (new TimestampStatistics())
    Statistics.sqlTypeToStatistics(BooleanType) should be (new BooleanStatistics())
    Statistics.sqlTypeToStatistics(ShortType) should be (new ShortStatistics())
    Statistics.sqlTypeToStatistics(ByteType) should be (new ByteStatistics())
  }

  test("select statistics for unsupported type") {
    val err = intercept[UnsupportedOperationException] {
      Statistics.sqlTypeToStatistics(NullType)
    }
    err.getMessage should be (s"No statistics for data type: $NullType")
  }

  test("unsupported statistics to magic conversion") {
    val err = intercept[UnsupportedOperationException] {
      Statistics.statisticsToMagic(new Statistics() {
        override def updateNonNullValue(row: InternalRow, ordinal: Int): Unit = ???
        override def writeState(buf: OutputBuffer): Unit = ???
        override def readState(buf: ByteBuffer): Unit = ???
        override def merge(obj: Statistics): Unit = ???
      })
    }
    assert(err.getMessage.contains("Unrecognized statistics"))
  }

  test("unsupported magic to statistics conversion") {
    val err = intercept[UnsupportedOperationException] {
      Statistics.magicToStatistics(-1)
    }
    err.getMessage should be ("Unrecognized statistics: -1")
  }

  test("statistics to magic conversion") {
    Statistics.statisticsToMagic(Statistics.magicToStatistics(1)) should be (1)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(2)) should be (2)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(3)) should be (3)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(4)) should be (4)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(5)) should be (5)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(6)) should be (6)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(7)) should be (7)
    Statistics.statisticsToMagic(Statistics.magicToStatistics(8)) should be (8)
  }
}
