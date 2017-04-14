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

package com.github.sadikovi.serde

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.serde.Converters._
import com.github.sadikovi.testutil.UnitTestSuite

class ConvertersSuite extends UnitTestSuite {
  test("converter for sql type") {
    Converters.sqlTypeToConverter(IntegerType) should be (new IndexedRowIntConverter())
    Converters.sqlTypeToConverter(LongType) should be (new IndexedRowLongConverter())
    Converters.sqlTypeToConverter(StringType) should be (new IndexedRowUTF8Converter())
  }

  test("converter for unsupported sql type") {
    val err = intercept[RuntimeException] {
      Converters.sqlTypeToConverter(NullType)
    }
    err.getMessage should be ("No converter registered for type NullType")
  }

  test("indexed row int converter") {
    val row = InternalRow(123, 124L, UTF8String.fromString("abcd"))
    val fixedBuffer = new OutputBuffer()
    val variableBuffer = new OutputBuffer()

    val cnv = new IndexedRowIntConverter()
    // 4 bytes for integer value
    cnv.byteOffset() should be (4)
    cnv.writeDirect(row, 0, fixedBuffer, 0, variableBuffer)
    fixedBuffer.array() should be (Array[Byte](
      0, 0, 0, 123
    ))
    assert(variableBuffer.array().isEmpty)
  }

  test("indexed row long converter") {
    val row = InternalRow(123, 124L, UTF8String.fromString("abcd"))
    val fixedBuffer = new OutputBuffer()
    val variableBuffer = new OutputBuffer()

    val cnv = new IndexedRowLongConverter()
    // 8 bytes for long value
    cnv.byteOffset() should be (8)
    cnv.writeDirect(row, 1, fixedBuffer, 4, variableBuffer)
    fixedBuffer.array() should be (Array[Byte](
      0, 0, 0, 0, 0, 0, 0, 124
    ))
    assert(variableBuffer.array().isEmpty)
  }

  test("indexed row UTF8String converter") {
    val row = InternalRow(123, 124L, UTF8String.fromString("abc"), UTF8String.fromString("bcde"))
    val fixedBuffer = new OutputBuffer()
    val variableBuffer = new OutputBuffer()

    val cnv = new IndexedRowUTF8Converter()
    // 8 bytes - 4 bytes offset + 4 bytes length
    cnv.byteOffset() should be (8)
    cnv.writeDirect(row, 2, fixedBuffer, 12, variableBuffer)
    cnv.writeDirect(row, 3, fixedBuffer, 12, variableBuffer)
    fixedBuffer.array() should be (Array[Byte](
      0, 0, 0, 12, 0, 0, 0, 3,
      0, 0, 0, 15, 0, 0, 0, 4
    ))
    variableBuffer.array() should be (Array[Byte](
      97, 98, 99,
      98, 99, 100, 101
    ))
  }
}
