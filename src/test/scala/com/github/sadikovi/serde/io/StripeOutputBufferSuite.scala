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

import java.io.{ByteArrayOutputStream, IOException}

import com.github.sadikovi.testutil.UnitTestSuite

class StripeOutputBufferSuite extends UnitTestSuite {
  test("init stripe output buffer") {
    val buf = new StripeOutputBuffer(123.toByte);
    buf.id() should be (123)
    buf.length() should be (0)
  }

  test("write data into buffer and check offset") {
    val buf = new StripeOutputBuffer(123.toByte);
    buf.id() should be (123)
    buf.length() should be (0)

    buf.write(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8), 2, 5)
    buf.length() should be (5)

    val out = new ByteArrayOutputStream()
    buf.flush(out)
    out.toByteArray should be (Array[Byte](3, 4, 5, 6, 7))
  }
}
