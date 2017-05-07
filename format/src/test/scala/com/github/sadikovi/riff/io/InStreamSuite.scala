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

import java.io.IOException

import com.github.sadikovi.testutil.UnitTestSuite

class InStreamSuite extends UnitTestSuite {
  test("uncompressed, init with input stream having fewer than HEADER_SIZE bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3))
    val in = new InStream(8, null, source)
    in.available() should be (3)
    in.read() should be (1)
    in.available() should be (2)
    in.read() should be (2)
    in.available() should be (1)
    in.read() should be (3)
    in.available() should be (0)
  }

  test("uncompressed, fail to read long with fewer bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3))
    val in = new InStream(8, null, source)
    in.available() should be (3)
    var err = intercept[IOException] {
      in.readLong()
    }
    err.getMessage should be ("EOF, 3 bytes != 8")
  }

  test("uncompressed, fail to read int with fewer bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3))
    val in = new InStream(8, null, source)
    in.available() should be (3)
    var err = intercept[IOException] {
      in.readInt()
    }
    err.getMessage should be ("EOF, 3 bytes != 4")
  }

  test("uncompressed, skip bytes and read") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val in = new InStream(4, null, source)
    in.skip(6)
    in.read() should be (7)
    in.skip(1)
    val out = Array[Byte](0, 0, 0, 0)
    in.read(out)
    out should be (Array[Byte](9, 10, 0, 0))
  }

  test("uncompressed, read negative amount bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4))
    val in = new InStream(4, null, source)
    val err = intercept[IndexOutOfBoundsException] {
      in.read(new Array[Byte](2), 0, -1)
    }
    err.getMessage should be ("Negative length: -1")
  }

  test("uncompressed, skip negative amount bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val in = new InStream(4, null, source)
    val err = intercept[IndexOutOfBoundsException] {
      in.skip(-1)
    }
    err.getMessage should be ("Negative length: -1")
  }

  test("uncompressed, read more than buffer size") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val in = new InStream(4, null, source)
    val out = new Array[Byte](10)
    in.read(out)
    out should be (Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  }

  test("uncompressed, skip more than buffer size") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val in = new InStream(4, null, source)
    in.skip(10)
    in.available() should be (0)
  }

  test("uncompressed, skip all remaining bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val in = new InStream(32, null, source)
    in.skip(10)
    in.available() should be (0)
  }

  test("compressed, init with input stream having fewer than HEADER_SIZE bytes") {
    // header is 4 bytes long
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3))
    val err = intercept[IOException] {
      new InStream(8, new ZlibCodec(), source)
    }
    assert(err.getMessage.contains("EOF"));
  }

  test("compressed, read simple compressed chunk") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0
    ))
    val in = new InStream(16, new ZlibCodec(), source)
    in.available() should be (16)
    in.read() should be (12)
    in.readInt() should be (125)
    in.readLong() should be (125L)
    in.available() should be (3)
    in.read() should be (2)
    in.read() should be (3)
    in.read() should be (4)
  }

  test("compressed, read simple uncompressed chunk") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](0, 0, 0, 4, 1, 2, 3, 4))
    val in = new InStream(8, new ZlibCodec(), source)
    in.available() should be (4)
    in.read() should be (1)
    in.available() should be (3)
    in.read() should be (2)
    in.available() should be (2)
    in.read() should be (3)
    in.available() should be (1)
    in.read() should be (4)
    in.available() should be (0)
  }

  test("compressed, read batch of compressed and uncompressed chunks") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      -128, 0, 0, 13,
      /* bytes: 12, 0, 0, 0, 125, 0, 0, 0, 0, 0, 0, 0, 125, 2, 3, 4 */
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0,
      0, 0, 0, 4,
      1, 2, 3, 4
    ))
    val in = new InStream(16, new ZlibCodec(), source)
    in.available() should be (16)
    in.read() should be (12)
    in.readInt() should be (125)
    in.readLong() should be (125L)
    // skip last 3 bytes which are 2, 3, 4
    in.skip(3)

    // at this point available() is checked based on source, as `uncompressed.remaining` is 0,
    // source has 4 (header) + 4 (data) bytes for the next chunk
    in.available() should be (4 + 4)
    in.read() should be (1)
    in.read() should be (2)
    in.read() should be (3)
    in.read() should be (4)
    in.available() should be (0)
  }

  test("compressed, skip more bytes than buffer size") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0,
      0, 0, 0, 4,
      1, 2, 3, 4
    ))
    val in = new InStream(16, new ZlibCodec(), source)
    in.skip(19)
    // skip to last byte, which is 4 (second chunk)
    in.read() should be (4)
  }

  test("compressed, skip all remaining bytes") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0
    ))
    val in = new InStream(64, new ZlibCodec(), source)
    in.skip(16)
    in.available() should be (0)
  }

  test("compressed, available when no bytes left in tmp buffer") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      0, 0, 0, 4,
      1, 2, 3, 4,
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0
    ))
    val in = new InStream(16, new ZlibCodec(), source)
    in.skip(4)
    // at this point in.available() is based on source as `uncompressed.remaining` is 0, which
    // results in 4 (header) + 13 (data) bytes
    in.available() should be (4 + 13)
  }

  test("mark/reset is unsupported") {
    val source = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4))
    val in = new InStream(4, null, source)
    in.markSupported() should be (false)
    intercept[UnsupportedOperationException] {
      in.mark(100)
    }
    intercept[UnsupportedOperationException] {
      in.reset()
    }
  }

  test("compressed, instream is refilled during readInt()") {
    // this test exposes issue when the same buffer was reused for reading header and would
    // overwrite previously written bytes
    val source = new StripeInputBuffer(1.toByte, Array[Byte](
      /* header */
      0, 0, 0, 3,
      0, 0, 0,
      -128, 0, 0, 13,
      /* bytes: 12, 0, 0, 0, 125, 0, 0, 0, 0, 0, 0, 0, 125, 2, 3, 4 */
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0
    ))
    val in = new InStream(16, new ZlibCodec(), source)
    in.readInt() should be (12)
  }
}
