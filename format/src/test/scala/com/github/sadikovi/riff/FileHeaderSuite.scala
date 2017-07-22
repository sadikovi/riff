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
import java.util.HashMap

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types._

import com.github.sadikovi.testutil.UnitTestSuite
import com.github.sadikovi.testutil.implicits._

class FileHeaderSuite extends UnitTestSuite {
  import RiffTestUtils._

  test("state length check") {
    var err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](1), null, null)
    }
    err.getMessage should be ("Invalid state length, 1 != 8")

    err = intercept[IllegalArgumentException] {
      new FileHeader(new Array[Byte](10), null, null)
    }
    err.getMessage should be ("Invalid state length, 10 != 8")

    // valid state length
    val header = new FileHeader(new Array[Byte](8), null, null)
    header.getTypeDescription should be (null)
    header.getProperty("key") should be (null)
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

      val header1 = new FileHeader(state, td, null)
      val out = fs.create(dir / "header")
      header1.writeTo(out)
      out.close()
      // read header and compare with original
      val in = fs.open(dir / "header")
      val header2 = FileHeader.readFrom(in)
      in.close()

      header2.getTypeDescription should be (header1.getTypeDescription)
      for (i <- 0 until state.length) {
        header2.state(i) should be (header1.state(i))
      }
    }
  }

  test("write/read file header with properties") {
    withTempDir { dir =>
      val td = new TypeDescription(StructType(StructField("col", IntegerType) :: Nil))
      val state = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)

      val props = new HashMap[String, String]()
      props.put("key1", "value1")
      props.put("key2", "value2")
      props.put("key3", "")

      val header1 = new FileHeader(state, td, props)
      val out = fs.create(dir / "header")
      header1.writeTo(out)
      out.close()
      // read header and compare with original
      val in = fs.open(dir / "header")
      val header2 = FileHeader.readFrom(in)
      in.close()

      header2.getTypeDescription should be (header1.getTypeDescription)
      for (i <- 0 until state.length) {
        header2.state(i) should be (header1.state(i))
      }
      header2.getProperty("key1") should be ("value1")
      header2.getProperty("key2") should be ("value2")
      header2.getProperty("key3") should be ("")
    }
  }
}
