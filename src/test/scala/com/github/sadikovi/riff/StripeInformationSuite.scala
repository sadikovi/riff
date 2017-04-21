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

package com.github.sadikovi.riff

import java.io.{ByteArrayInputStream, IOException}

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.riff.io.StripeOutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class StripeInformationSuite extends UnitTestSuite {
  test("init stripe information from stripe output buffer") {
    val buf = new StripeOutputBuffer(123.toByte)
    buf.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8), 0, 8)
    val info = new StripeInformation(buf, 12345L)
    info.id() should be (123)
    info.offset() should be (12345L)
    info.length() should be (8)
  }

  test("toString method") {
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue)
    info1.toString should be (s"Stripe[id=123, offset=12345, length=${Int.MaxValue}]")
  }

  test("assert negative values in stripe information") {
    var err = intercept[IllegalArgumentException] {
      new StripeInformation(-1.toByte, 1L, 1)
    }
    err.getMessage should be ("Negative id: -1")

    err = intercept[IllegalArgumentException] {
      new StripeInformation(1.toByte, -1L, 1)
    }
    err.getMessage should be ("Negative offset: -1")

    err = intercept[IllegalArgumentException] {
      new StripeInformation(1.toByte, 1L, -1)
    }
    err.getMessage should be ("Negative length: -1")
  }

  test("fail magic assertion") {
    val in = new ByteArrayInputStream(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val err = intercept[IOException] {
      StripeInformation.readExternal(in)
    }
    err.getMessage should be ("Wrong magic number for stripe information")
  }

  test("write/read external") {
    val out = new OutputBuffer()
    val info1 = new StripeInformation(123.toByte, 12345L, Int.MaxValue)
    info1.writeExternal(out)

    val in = new ByteArrayInputStream(out.array())
    val info2 = StripeInformation.readExternal(in)

    info2.id() should be (info1.id())
    info2.offset() should be (info1.offset())
    info2.length() should be (info1.length())
    info2.toString() should be (info1.toString())
  }
}
