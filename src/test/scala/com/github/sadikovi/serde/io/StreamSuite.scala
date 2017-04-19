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

package com.github.sadikovi.serde.io

import java.io.ByteArrayOutputStream

import com.github.sadikovi.testutil.UnitTestSuite

/**
 * StreamSuite is an integration test suite for InStream - OutStream
 *
 */
class StreamSuite extends UnitTestSuite {
  test("save compressed outstream and load instream") {
    val buf = new ByteArrayOutputStream()
    val out = new OutStream(8, new ZlibCodec(), buf)
    out.writeLong(121L)
    out.writeLong(122L)
    out.writeLong(123L)
    out.writeInt(21)
    out.writeInt(22)
    out.writeInt(23)
    out.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    out.flush()

    val in = new InStream(8, new ZlibCodec(), new StripeInputBuffer(1.toByte, buf.toByteArray))
    in.readLong() should be (121L)
    in.readLong() should be (122L)
    in.readLong() should be (123L)
    in.readInt() should be (21)
    in.readInt() should be (22)
    in.readInt() should be (23)
    val arr = new Array[Byte](8)
    in.read(arr)
    arr should be (Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    in.read() should be (9)
    in.read() should be (10)
    in.available() should be (0)
  }

  test("save uncompressed outstream and load instream") {
    val buf = new ByteArrayOutputStream()
    val out = new OutStream(8, null, buf)
    out.writeLong(121L)
    out.writeLong(122L)
    out.writeLong(123L)
    out.writeInt(21)
    out.writeInt(22)
    out.writeInt(23)
    out.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    out.flush()

    val in = new InStream(8, null, new StripeInputBuffer(1.toByte, buf.toByteArray))
    in.readLong() should be (121L)
    in.readLong() should be (122L)
    in.readLong() should be (123L)
    in.readInt() should be (21)
    in.readInt() should be (22)
    in.readInt() should be (23)
    val arr = new Array[Byte](8)
    in.read(arr)
    arr should be (Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    in.read() should be (9)
    in.read() should be (10)
    in.available() should be (0)
  }
}
