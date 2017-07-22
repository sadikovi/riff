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

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.hadoop.fs.FSDataInputStream

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.riff.stats.Statistics
import com.github.sadikovi.testutil.UnitTestSuite
import com.github.sadikovi.testutil.implicits._

class FileFooterSuite extends UnitTestSuite {
  import RiffTestUtils._

  test("check wrong number of records") {
    var err = intercept[IllegalArgumentException] {
      new FileFooter(Array.empty[Statistics], -1, Array.empty[StripeInformation])
    }
    err.getMessage should be ("Negative number of records: -1")

    err = intercept[IllegalArgumentException] {
      new FileFooter(Array.empty[Statistics], Int.MinValue, Array.empty[StripeInformation])
    }
    err.getMessage should be (s"Negative number of records: ${Int.MinValue}")
  }

  test("fail if stripes and buffer are null") {
    val footer = new FileFooter(Array.empty, 1, null, null)
    val err = intercept[IllegalArgumentException] {
      footer.getStripeInformation()
    }
    err.getMessage should be ("Byte buffer is null, cannot reconstruct stripe information")
  }

  test("get stripe information, stripes are not null") {
    var footer = new FileFooter(Array.empty, 1, Array.empty, null)
    footer.getStripeInformation() should be (Array.empty)

    val stripe = new StripeInformation(1.toByte, 123L, 100, null)
    footer = new FileFooter(Array.empty, 1, Array(stripe), null)
    footer.getStripeInformation() should be (Array(stripe))
  }

  test("get stripe information, stripes are null") {
    val out = new OutputBuffer()
    val stripe = new StripeInformation(1.toByte, 123L, 100, null)
    out.writeInt(1) // write length of the stripe array
    stripe.writeExternal(out)
    val buffer = ByteBuffer.wrap(out.array())

    val footer = new FileFooter(Array.empty, 1, null, buffer)
    footer.getStripeInformation() should be (Array(stripe))
    // try extracting it again, should return the same value
    footer.getStripeInformation() should be (Array(stripe))
  }

  test("check wrong magic number") {
    withTempDir { dir =>
      val out = create(dir / "header")
      out.write(Array[Byte](0, 0, 0, 1, 0, 0, 0, 2))
      out.close()
      val in = open(dir / "header").asInstanceOf[FSDataInputStream]
      val status = fs.getFileStatus(dir / "header")
      val err = intercept[IOException] {
        FileFooter.readFrom(in, status.getLen)
      }
      assert(err.getMessage.contains("Wrong magic"))
      in.close()
    }

  }

  test("write/read file footer") {
    withTempDir { dir =>
      val stripe = new StripeInformation(1.toByte, 123L, 100, null)
      val footer1 = new FileFooter(Array(stats(1, 10, false)), 156378, Array(stripe))

      val out = fs.create(dir / "header")
      footer1.writeTo(out)
      out.close()
      // read footer and compare with original
      val in = fs.open(dir / "header")
      val status = fs.getFileStatus(dir / "header")
      val footer2 = FileFooter.readFrom(in, status.getLen)
      in.close()

      footer2.getFileStatistics should be (footer1.getFileStatistics)
      footer2.getNumRecords should be (footer1.getNumRecords)
      footer2.getStripeInformation should be (footer1.getStripeInformation)
    }
  }
}
