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

import java.io.IOException
import java.nio.ByteBuffer

import com.github.sadikovi.testutil.UnitTestSuite

class StripeInputBufferSuite extends UnitTestSuite {
  test("init with empty data array") {
    var err = intercept[IllegalArgumentException] {
      new StripeInputBuffer(1.toByte, null)
    }
    err.getMessage should be ("Empty data for stripe")

    err = intercept[IllegalArgumentException] {
      new StripeInputBuffer(1.toByte, new Array[Byte](0))
    }
    err.getMessage should be ("Empty data for stripe")
  }

  test("initial configuration") {
    val buf = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    buf.length should be (8)
    buf.position should be (0)
    buf.id should be (1)
  }

  test("toString method after close") {
    val buf = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    buf.toString should be ("StripeInput[id=1, offset=0, closed=false]")
    buf.close()
    buf.toString should be ("StripeInput[id=1, offset=0, closed=true]")
  }

  test("seek with invalid position") {
    val buf = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    var err = intercept[IOException] {
      buf.seek(-1)
    }
    assert(err.getMessage.contains("Invalid position -1"))

    err = intercept[IOException] {
      buf.seek(10)
    }
    assert(err.getMessage.contains("Invalid position 10"))

    buf.seek(4) // valid position
    err = intercept[IOException] {
      // should not support position decrement
      buf.seek(3)
    }
    assert(err.getMessage.contains("Cannot set position 3"))
  }

  test("copy data into buffer") {
    val buf = new StripeInputBuffer(1.toByte, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    val out = ByteBuffer.allocate(5)
    buf.copy(out)
    buf.position() should be (5)
    out.array() should be (Array[Byte](1, 2, 3, 4, 5))

    out.clear()
    buf.copy(out)
    buf.position() should be (8)
    // should copy bytes partially, overwrites values already in the buffer
    out.array() should be (Array[Byte](6, 7, 8, 4, 5))
  }
}
