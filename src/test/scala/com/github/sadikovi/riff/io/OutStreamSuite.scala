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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.github.sadikovi.testutil.UnitTestSuite

class OutStreamSuite extends UnitTestSuite {
  test("outstream - write uncompressed") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val out = new OutStream(bufferSize, null, receiver)
    out.write(12)
    out.writeInt(125)
    out.writeLong(125L)
    out.write(Array[Byte](1, 2, 3, 4, 5), 1, 3)
    out.flush()
    out.close()

    // no header is written
    receiver.array() should be (Array[Byte](
      12,
      0, 0, 0, 125,
      0, 0, 0, 0, 0, 0, 0, 125,
      2, 3, 4
    ))
  }

  test("outstream - write uncompressed, no bytes written") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val out = new OutStream(bufferSize, null, receiver)
    out.flush()
    out.close()
    // byte array should be empty
    receiver.array() should be (Array[Byte]())
  }

  test("outstream - write compressed, no bytes written") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val out = new OutStream(bufferSize, new ZlibCodec(), receiver)
    out.flush()
    out.close()
    // byte array should contain only header, in this case should have 0 compression flag and 0
    // bytes written
    receiver.array() should be (Array[Byte](0, 0, 0, 0))
  }

  test("outstream - write compressed zlib, use compression") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val codec = new ZlibCodec()
    val out = new OutStream(bufferSize, codec, receiver)
    out.write(12)
    out.writeInt(125)
    out.writeLong(125L)
    out.write(Array[Byte](1, 2, 3, 4, 5), 1, 3)
    out.flush()
    out.close()

    receiver.array() should be (Array[Byte](
      /* header */
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 38, 102, 22, 0
    ))

    // zlib codec should decompress this into input sequence of bytes
    val arr = receiver.array()
    // skip header for direct decompression
    val inbuf = ByteBuffer.wrap(arr, OutStream.HEADER_SIZE, arr.length - OutStream.HEADER_SIZE)
    val outbuf = ByteBuffer.allocate(32)
    codec.decompress(inbuf, outbuf)

    val output = new ByteArrayOutputStream()
    output.write(outbuf.array(), outbuf.arrayOffset() + outbuf.position(), outbuf.remaining())
    // result should be the same as writing uncompressed bytes
    output.toByteArray() should be (Array[Byte](
      12,
      0, 0, 0, 125,
      0, 0, 0, 0, 0, 0, 0, 125,
      2, 3, 4
    ))
  }

  test("outstream - write compressed zlib, fall back to uncompressed") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val codec = new ZlibCodec()
    val out = new OutStream(bufferSize, codec, receiver)
    out.write(1)
    out.write(2)
    out.write(3)
    out.write(4)
    out.write(5)
    out.write(Array[Byte](1, 2, 3, 4, 5), 1, 3)
    out.flush()
    out.close()

    receiver.array() should be (Array[Byte](
      /* header: 0 for uncompressed and 8 bytes written */
      0, 0, 0, 8,
      1, 2, 3, 4, 5, 2, 3, 4
    ))
  }

  test("outstream - write compressed zlib, write 2 chunks") {
    // write 2 chunks: compressed and uncompressed
    val receiver = new StripeOutputBuffer(1.toByte)
    val bufferSize = 16
    val codec = new ZlibCodec()
    val out = new OutStream(bufferSize, codec, receiver)
    out.write(12)
    out.writeInt(125)
    out.writeLong(125L)
    out.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    out.flush()
    out.close()

    receiver.array() should be (Array[Byte](
      /* header: 1 for compressed + 13 bytes of data */
      -128, 0, 0, 13,
      -29, 97, 96, 96, -88, 101, -128, -128, 90, 70, 38, 102, 0,
      /* header: 0 for uncompressed + 7 bytes of data */
      0, 0, 0, 7,
      4, 5, 6, 7, 8, 9, 10
    ))
  }

  test("outstream - write header, max value + compressed") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val buf = ByteBuffer.allocate(8)
    OutStream.writeHeader(buf, 0, Int.MaxValue, true)
    // do not flip buffer, outstram calls absolute `putInt`
    receiver.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
    receiver.array should be (Array[Byte](
      -1, -1, -1, -1, 0, 0, 0, 0
    ))
  }

  test("outstream - write header, max value - compressed") {
    val receiver = new StripeOutputBuffer(1.toByte)
    val buf = ByteBuffer.allocate(8)
    OutStream.writeHeader(buf, 0, Int.MaxValue, false)
    // do not flip buffer, outstram calls absolute `putInt`
    receiver.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
    receiver.array should be (Array[Byte](
      127, -1, -1, -1, 0, 0, 0, 0
    ))
  }

  test("outstream - write header, min value") {
    val buf = ByteBuffer.allocate(8)
    val err = intercept[IllegalArgumentException] {
      OutStream.writeHeader(buf, 0, Int.MinValue, true)
    }
    err.getMessage should be (s"Invalid bytes ${Int.MinValue}")
  }
}
