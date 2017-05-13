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

class SnappyCodecSuite extends UnitTestSuite {
  test("fill up compressed and overflow buffers and exit loop correctly") {
    val inbuf = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    val outbuf = ByteBuffer.allocate(8)
    val overflow = ByteBuffer.allocate(8)
    val codec = new SnappyCodec()
    codec.compress(inbuf, outbuf, overflow) should be (false)
    outbuf.remaining() should be (0)
    overflow.remaining() should be (0)
  }

  test("compress and decompress byte array with large result buffer") {
    val inbuf = ByteBuffer.allocate(128)
    while (inbuf.remaining() != 0) {
      inbuf.putLong(inbuf.remaining())
    }
    inbuf.flip()
    val outbuf = ByteBuffer.allocate(128)
    val codec = new SnappyCodec()
    codec.compress(inbuf, outbuf, null) should be (true)
    outbuf.flip()

    // result buffer is larger than original input buffer to test full read
    val resbuf = ByteBuffer.allocate(256)
    codec.decompress(outbuf, resbuf)
    resbuf.array().slice(0, 128) should be (inbuf.array())
  }

  test("compress and decompress on buffer size boundary") {
    val inbuf = ByteBuffer.allocate(128)
    while (inbuf.remaining() != 0) {
      inbuf.putLong(inbuf.remaining())
    }
    inbuf.flip()
    val outbuf = ByteBuffer.allocate(128)
    val codec = new SnappyCodec()
    codec.compress(inbuf, outbuf, null) should be (true)
    outbuf.flip()

    val resbuf = ByteBuffer.allocate(128)
    codec.decompress(outbuf, resbuf)
    resbuf.array() should be (inbuf.array())
  }

  test("compress and decompress with overflow spill") {
    val inbuf = ByteBuffer.allocate(128)
    while (inbuf.remaining() != 0) {
      inbuf.putLong(inbuf.remaining())
    }
    inbuf.flip()
    // there are 65 bytes in total
    val outbuf = ByteBuffer.allocate(32)
    val overflow = ByteBuffer.allocate(64)
    val codec = new SnappyCodec()
    codec.compress(inbuf, outbuf, overflow) should be (true)
    outbuf.flip()
    overflow.flip()
    val mergebuf = ByteBuffer.allocate(outbuf.remaining + overflow.remaining)
    mergebuf.put(outbuf)
    mergebuf.put(overflow)
    mergebuf.flip()

    val resbuf = ByteBuffer.allocate(128)
    codec.decompress(mergebuf, resbuf)
    resbuf.array() should be (inbuf.array())
  }

  test("compress and decompress byte array using smaller output buffer") {
    val inbuf = ByteBuffer.allocate(128)
    while (inbuf.remaining() != 0) {
      inbuf.putLong(inbuf.remaining())
    }
    inbuf.flip()
    val outbuf = ByteBuffer.allocate(128)
    val codec = new SnappyCodec()
    codec.compress(inbuf, outbuf, null) should be (true)
    outbuf.flip()

    val resbuf = ByteBuffer.allocate(64)
    val err = intercept[IOException] {
      codec.decompress(outbuf, resbuf)
    }
    err.getMessage should be ("Output buffer is too short, could not insert more bytes from " +
      "compressed byte buffer")
  }
}
