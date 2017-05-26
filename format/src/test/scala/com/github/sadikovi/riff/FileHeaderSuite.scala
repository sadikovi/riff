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

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types._

import com.github.sadikovi.riff.stats.Statistics
import com.github.sadikovi.testutil.UnitTestSuite
import com.github.sadikovi.testutil.implicits._

class FileHeaderSuite extends UnitTestSuite {
  import RiffTestUtils._

  test("state length check") {
    var err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](1), null, null, 0)
    }
    err.getMessage should be ("Invalid state length, 1 != 8")

    err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](10), null, null, 0)
    }
    err.getMessage should be ("Invalid state length, 10 != 8")

    // valid state length
    val header = new FileHeader(new Array[Byte](8), null, null, 0)
    header.getTypeDescription should be (null)
    header.getFileStatistics should be (null)
    header.getNumRecords should be (0)
  }

  test("check wrong number of records") {
    var err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](8), null, null, -1)
    }
    err.getMessage should be ("Negative number of records: -1")

    err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](8), null, null, Int.MinValue)
    }
    err.getMessage should be (s"Negative number of records: ${Int.MinValue}")
  }

  test("check wrong magic number") {
    withTempDir { dir =>
      val out = create(dir / "header")
      out.write(Array[Byte](0, 0, 0, 1, 0, 0, 0, 2))
      out.close()
      val in = open(dir / "header").asInstanceOf[FSDataInputStream]
      val err = intercept[IOException] {
        FileHeader.readFrom(in)
      }
      assert(err.getMessage.contains("Wrong magic"))
      in.close()
    }

  }

  test("write/read file header") {
    withTempDir { dir =>
      val td = new TypeDescription(StructType(StructField("col", IntegerType) :: Nil))
      val state = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
      val list = Array[Statistics](
        stats(1, 10, false)
      )
      val header1 = new FileHeader(state, td, list, 156378)
      val out = fs.create(dir / "header")
      header1.writeTo(out)
      out.close()
      // read header and compare with original
      val in = fs.open(dir / "header")
      val header2 = FileHeader.readFrom(in)
      in.close()

      header2.getTypeDescription should be (header1.getTypeDescription)
      header2.getFileStatistics should be (header1.getFileStatistics)
      header2.getNumRecords should be (156378)
      for (i <- 0 until state.length) {
        header2.state(i) should be (header1.state(i))
      }
    }
  }
}
