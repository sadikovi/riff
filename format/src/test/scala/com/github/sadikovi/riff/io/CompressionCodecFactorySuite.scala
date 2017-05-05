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

class CompressionCodecFactorySuite extends UnitTestSuite {
  test("encode codec into flag") {
    CompressionCodecFactory.encode(CompressionCodecFactory.UNCOMPRESSED) should be (0)
    CompressionCodecFactory.encode(new ZlibCodec()) should be (1)
    CompressionCodecFactory.encode(new GzipCodec()) should be (2)
  }

  test("encode unknown codec") {
    val codec = new CompressionCodec() {
      override def compress(in: ByteBuffer, out: ByteBuffer, overflow: ByteBuffer): Boolean = false
      override def decompress(in: ByteBuffer, out: ByteBuffer): Unit = { }
      override def reset(): Unit = { }
      override def close(): Unit = { }
    }
    val err = intercept[UnsupportedOperationException] {
      CompressionCodecFactory.encode(codec)
    }
    err.getMessage should be (s"Unknown codec: $codec")
  }

  test("decode flag into codec") {
    assert(CompressionCodecFactory.decode(0) == CompressionCodecFactory.UNCOMPRESSED)
    assert(CompressionCodecFactory.decode(1).isInstanceOf[ZlibCodec])
    assert(CompressionCodecFactory.decode(2).isInstanceOf[GzipCodec])
  }

  test("decode unknown flag") {
    val err = intercept[UnsupportedOperationException] {
      CompressionCodecFactory.decode(-1)
    }
    err.getMessage should be ("Unknown codec flag: -1")
  }

  test("codec for short name") {
    assert(CompressionCodecFactory.forShortName("none") == CompressionCodecFactory.UNCOMPRESSED)
    assert(CompressionCodecFactory.forShortName("deflate").isInstanceOf[ZlibCodec])
    assert(CompressionCodecFactory.forShortName("gzip").isInstanceOf[GzipCodec])

    assert(CompressionCodecFactory.forShortName("NONE") == CompressionCodecFactory.UNCOMPRESSED)
    assert(CompressionCodecFactory.forShortName("DEFLATE").isInstanceOf[ZlibCodec])
    assert(CompressionCodecFactory.forShortName("GZIP").isInstanceOf[GzipCodec])
  }

  test("codec for unknown short name") {
    val err = intercept[UnsupportedOperationException] {
      CompressionCodecFactory.forShortName("unknown")
    }
    err.getMessage should be ("Unknown codec: unknown")
  }

  test("codec for file extension") {
    assert(CompressionCodecFactory.forFileExt(".deflate").isInstanceOf[ZlibCodec])
    assert(CompressionCodecFactory.forFileExt(".gz").isInstanceOf[GzipCodec])

    assert(CompressionCodecFactory.forFileExt("none") == CompressionCodecFactory.UNCOMPRESSED)
    assert(CompressionCodecFactory.forFileExt("") == CompressionCodecFactory.UNCOMPRESSED)
  }
}
