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
import java.nio.ByteBuffer

import com.github.sadikovi.testutil.UnitTestSuite

class ZlibCodecSuite extends UnitTestSuite {
  test("fill up compressed and overflow buffers and exit loop correctly") {
    val inbuf = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    val outbuf = ByteBuffer.allocate(4)
    val overflow = ByteBuffer.allocate(4)
    val codec = new ZlibCodec()
    codec.compress(inbuf, outbuf, overflow) should be (false)
    outbuf.remaining() should be (0)
    overflow.remaining() should be (0)
  }

  test("compress and decompress byte array") {
    val inbuf = ByteBuffer.allocate(32)
    inbuf.putInt(4)
    inbuf.putLong(8L)
    inbuf.putLong(12L)
    inbuf.flip()
    val outbuf = ByteBuffer.allocate(32)
    val codec = new ZlibCodec()
    codec.compress(inbuf, outbuf, null) should be (true)
    outbuf.flip()

    val resbuf = ByteBuffer.allocate(32)
    codec.decompress(outbuf, resbuf)
    resbuf.array() should be (inbuf.array())
  }

  test("compress and decompress byte array using smaller output buffer") {
    // this test checks that we can still decompress when output buffer has smaller size that
    // actual size of decompressed bytes
    val inbuf = ByteBuffer.allocate(32)
    inbuf.putInt(4)
    inbuf.putLong(8L)
    inbuf.putLong(12L)
    inbuf.flip()
    val outbuf = ByteBuffer.allocate(32)
    val codec = new ZlibCodec()
    codec.compress(inbuf, outbuf, null) should be (true)
    outbuf.flip()

    val resbuf = ByteBuffer.allocate(8)
    val err = intercept[IOException] {
      codec.decompress(outbuf, resbuf)
    }
    err.getMessage should be ("Output buffer is too short, could not insert more bytes from " +
      "compressed byte buffer")
  }

  test("double close zlib codec") {
    val codec = new ZlibCodec()
    codec.close()
    // next close should be no-op
    codec.close()
  }
}
