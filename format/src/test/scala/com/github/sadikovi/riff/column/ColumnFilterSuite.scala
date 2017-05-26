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
import org.apache.spark.sql.types._

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class ColumnFilterSuite extends UnitTestSuite {
  test("select filter for sql type") {
    assert(ColumnFilter.sqlTypeToColumnFilter(BooleanType, 256).isInstanceOf[BooleanColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(ByteType, 256).isInstanceOf[ByteColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(ShortType, 256).isInstanceOf[ShortColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(IntegerType, 256).isInstanceOf[IntColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(LongType, 256).isInstanceOf[LongColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(StringType, 256).isInstanceOf[UTF8ColumnFilter])
    assert(ColumnFilter.sqlTypeToColumnFilter(DateType, 256).isInstanceOf[DateColumnFilter])
    assert(
      ColumnFilter.sqlTypeToColumnFilter(TimestampType, 256).isInstanceOf[TimestampColumnFilter])
    // no-op filter if type is not supported
    assert(ColumnFilter.sqlTypeToColumnFilter(NullType, 256).isInstanceOf[NoopColumnFilter])
  }

  test("column filter to magic") {
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(1)) should be (1)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(2)) should be (2)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(3)) should be (3)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(4)) should be (4)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(5)) should be (5)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(6)) should be (6)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(7)) should be (7)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(8)) should be (8)
    ColumnFilter.columnFilterToMagic(ColumnFilter.magicToColumnFilter(9)) should be (9)
  }

  test("fail to select column filters to magic") {
    val err = intercept[UnsupportedOperationException] {
      ColumnFilter.columnFilterToMagic(new ColumnFilter() {
        override def updateNonNullValue(row: InternalRow, ordinal: Int): Unit = { }
        override def writeState(buffer: OutputBuffer): Unit = { }
        override def readState(buffer: ByteBuffer): Unit = { }
      })
    }
    assert(err.getMessage.contains("Unrecognized column filter"))
  }

  test("fail to select magic to column filter") {
    var err = intercept[UnsupportedOperationException] { ColumnFilter.magicToColumnFilter(0) }
    assert(err.getMessage.contains("Unrecognized column filter magic: 0"))

    err = intercept[UnsupportedOperationException] { ColumnFilter.magicToColumnFilter(-1) }
    assert(err.getMessage.contains("Unrecognized column filter magic: -1"))

    err = intercept[UnsupportedOperationException] { ColumnFilter.magicToColumnFilter(10) }
    assert(err.getMessage.contains("Unrecognized column filter magic: 10"))
  }
}
