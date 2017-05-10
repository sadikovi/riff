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

import java.io.ByteArrayInputStream

import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class FSAppendStreamSuite extends UnitTestSuite {
  test("init with wrong copy buffer size") {
    val err = intercept[IllegalArgumentException] {
      new FSAppendStream(null, -1)
    }
    err.getMessage should be ("Negative buffer size: -1")
  }

  test("write stream") {
    val in = new ByteArrayInputStream(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    val out = new OutputBuffer()
    val stream = new FSAppendStream(out, 4)
    stream.writeStream(in)
    stream.flush()
    out.array should be (Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
  }

  test("write empty stream") {
    val in = new ByteArrayInputStream(Array[Byte]())
    val out = new OutputBuffer()
    val stream = new FSAppendStream(out, 4)
    stream.writeStream(in)
    stream.flush()
    out.array should be (Array[Byte]())
  }

  test("write stream from file") {
    withTempDir { dir =>
      val out = create(dir / "file")
      out.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      out.close()

      val in = open(dir / "file")
      val buf = new OutputBuffer()
      val stream = new FSAppendStream(buf, 4)
      stream.writeStream(in)
      in.close()
      stream.close()
      buf.array should be (Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    }
  }

  test("write empty stream from file") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file")
      val buf = new OutputBuffer()
      val stream = new FSAppendStream(buf, 4)
      stream.writeStream(in)
      in.close()
      stream.close()
      buf.array should be (Array[Byte]())
    }
  }
}
