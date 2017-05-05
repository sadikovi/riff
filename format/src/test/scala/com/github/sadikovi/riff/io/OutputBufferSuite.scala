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

package com.github.sadikovi.riff.io

import com.github.sadikovi.testutil.UnitTestSuite

class OutputBufferSuite extends UnitTestSuite {
  test("output buffer - writeBoolean") {
    val buf = new OutputBuffer()
    buf.writeBoolean(false)
    buf.writeBoolean(true)
    buf.bytesWritten() should be (2)
    buf.array() should be (Array[Byte](0, 1))
  }

  test("output buffer - writeByte") {
    val buf = new OutputBuffer()
    buf.writeByte(1.toByte)
    buf.writeByte(127.toByte)
    buf.writeByte(-128.toByte)
    buf.writeByte(32.toByte)
    buf.bytesWritten() should be (4)
    buf.array() should be (Array[Byte](1, 127, -128, 32))
  }

  test("output buffer - writeDouble") {
    val buf = new OutputBuffer()
    buf.writeDouble(1.0)
    buf.writeDouble(1.2)
    buf.writeDouble(257.123)
    buf.bytesWritten() should be (24)
    buf.array() should be (Array[Byte](
      63, -16, 0, 0, 0, 0, 0, 0,
      63, -13, 51, 51, 51, 51, 51, 51,
      64, 112, 17, -9, -50, -39, 22, -121
    ))
  }

  test("output buffer - writeFloat") {
    val buf = new OutputBuffer()
    buf.writeFloat(1.0f)
    buf.writeFloat(1.2f)
    buf.writeFloat(257.123f)
    buf.bytesWritten() should be (12)
    buf.array() should be (Array[Byte](
      63, -128, 0, 0,
      63, -103, -103, -102,
      67, -128, -113, -66
    ))
  }

  test("output buffer - writeInt") {
    val buf = new OutputBuffer()
    buf.writeInt(Int.MinValue)
    buf.writeInt(255)
    buf.writeInt(128234)
    buf.writeInt(Int.MaxValue)
    buf.bytesWritten() should be (16)
    buf.array() should be (Array[Byte](
      -128, 0, 0, 0,
      0, 0, 0, -1,
      0, 1, -12, -22,
      127, -1, -1, -1
    ))
  }

  test("output buffer - writeLong") {
    val buf = new OutputBuffer()
    buf.writeLong(Long.MinValue)
    buf.writeLong(255L)
    buf.writeLong(128234L)
    buf.writeLong(Int.MaxValue.toLong)
    buf.bytesWritten() should be (32)
    buf.array() should be (Array[Byte](
      -128, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, -1,
      0, 0, 0, 0, 0, 1, -12, -22, 0,
      0, 0, 0, 127, -1, -1, -1
    ))
  }

  test("output buffer - writeShort") {
    val buf = new OutputBuffer()
    buf.writeShort(Short.MinValue)
    buf.writeShort(128.toShort)
    buf.writeShort(255.toShort)
    buf.writeShort(32498.toShort)
    buf.writeShort(Short.MaxValue)
    buf.bytesWritten() should be (10)
    buf.array() should be (Array[Byte](
      -128, 0,
      0, -128,
      0, -1,
      126, -14,
      127, -1
    ))
  }

  test("output buffer - writeBytes") {
    val arr = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val buf = new OutputBuffer()
    buf.writeBytes(arr, 4, 3)
    buf.bytesWritten() should be (3)
    buf.array() should be (Array[Byte](5, 6, 7))

    buf.reset()
    buf.writeBytes(arr)
    buf.bytesWritten() should be (arr.length)
    buf.array() should be (arr)
  }

  test("output buffer - write/reset/write") {
    val buf = new OutputBuffer()
    buf.writeBoolean(true)
    buf.writeInt(123)
    buf.writeBytes(Array[Byte](1, 2, 3, 4))
    buf.writeBytes(Array[Byte](1, 2, 3, 4), 2, 1)
    buf.bytesWritten() should be (1 + 4 + 4 + 1)
    buf.array() should be (Array[Byte](
      1,
      0, 0, 0, 123,
      1, 2, 3, 4, 3
    ))

    buf.reset()
    buf.bytesWritten() should be (0)
    buf.array() should be (Array[Byte]())

    buf.writeLong(123L)
    buf.writeBytes(Array[Byte](9, 8, 7, 6, 5, 4, 3, 2))
    buf.bytesWritten() should be (16)
    buf.array() should be (Array[Byte](
      0, 0, 0, 0, 0, 0, 0, 123,
      9, 8, 7, 6, 5, 4, 3, 2
    ))
  }
}
