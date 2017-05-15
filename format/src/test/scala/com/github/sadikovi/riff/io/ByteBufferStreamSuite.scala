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

import java.nio.ByteBuffer

import com.github.sadikovi.testutil.UnitTestSuite

class ByteBufferStreamSuite extends UnitTestSuite {
  test("stream on empty byte buffer") {
    val in = new ByteBufferStream(ByteBuffer.allocate(0))
    in.available should be (0)
    in.read(new Array[Byte](4)) should be (0)
    // read of single byte when no bytes are left
    in.read() should be (-1)
  }

  test("stream on byte buffer") {
    val buf = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val in = new ByteBufferStream(buf)
    in.available should be (8)
    in.read(new Array[Byte](5)) should be (5)
    in.available should be (3)
    in.read() should be (6)
    in.available should be (2)
    // read more than remaining
    in.read(new Array[Byte](3)) should be (2)
    in.available should be (0)
  }

  test("stream - skip bytes more than buffer") {
    val buf = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val in = new ByteBufferStream(buf)
    in.skip(100) should be (8)
    in.available should be (0)
  }

  test("stream - skip bytes less than buffer") {
    val buf = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val in = new ByteBufferStream(buf)
    in.skip(6) should be (6)
    in.available should be (2)
    in.skip(6) should be (2)
    in.available should be (0)
  }
}
