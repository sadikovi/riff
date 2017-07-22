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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class FileReaderSuite extends UnitTestSuite {
  import com.github.sadikovi.riff.RiffTestUtils._

  val schema = StructType(
    StructField("col1", IntegerType) ::
    StructField("col2", StringType) ::
    StructField("col3", LongType) :: Nil)

  val td = new TypeDescription(schema, Array("col2"))

  test("initialize file reader for non-existent path") {
    withTempDir { dir =>
      val path = dir / "file"
      touch(path)
      val reader = new FileReader(fs, new Configuration(), path)
      reader.filePath() should be (new Path(s"file:$path"))
      reader.bufferSize() should be (Riff.Options.BUFFER_SIZE_DEFAULT)
    }
  }

  test("initialize file reader with different buffer size") {
    withTempDir { dir =>
      val path = dir / "file"
      touch(path)
      val conf = new Configuration()
      conf.setInt(Riff.Options.BUFFER_SIZE, Riff.Options.BUFFER_SIZE_MAX)
      val reader = new FileReader(fs, conf, path)
      reader.filePath() should be (new Path(s"file:$path"))
      reader.bufferSize() should be (Riff.Options.BUFFER_SIZE_MAX)
    }
  }

  test("evaluate stripes for null predicate state") {
    val stripes = Array(
      new StripeInformation(1.toByte, 0L, 100, null),
      new StripeInformation(2.toByte, 101L, 100, null),
      new StripeInformation(3.toByte, 202L, 100, null))
    val res = FileReader.evaluateStripes(stripes, null)
    res should be (stripes)
  }

  test("evaluate stripes for predicate state and no statistics") {
    val stripes = Array(
      new StripeInformation(2.toByte, 101L, 100, null),
      new StripeInformation(1.toByte, 0L, 100, null),
      new StripeInformation(3.toByte, 202L, 100, null))
    val state = new PredicateState(nvl("col1"), td)
    val res = FileReader.evaluateStripes(stripes, state)
    // must be sorted by offset
    res should be (Array(
      new StripeInformation(1.toByte, 0L, 100, null),
      new StripeInformation(2.toByte, 101L, 100, null),
      new StripeInformation(3.toByte, 202L, 100, null)))
  }

  test("evaluate stripes for predicate state - remove some stripes") {
    val stripes = Array(
      new StripeInformation(2.toByte, 101L, 100, Array(
        stats("a", "z", false),
        stats(1, 3, false),
        stats(1L, 3L, false)
      )),
      new StripeInformation(1.toByte, 0L, 100, Array(
        stats("a", "z", false),
        stats(4, 5, false),
        stats(1L, 3L, false)
      )),
      new StripeInformation(3.toByte, 202L, 100, Array(
        stats("a", "z", false),
        stats(1, 3, false),
        stats(1L, 3L, false)
      )))
    val state = new PredicateState(eqt("col1", 5), td)
    val res = FileReader.evaluateStripes(stripes, state)
    // must be sorted by offset
    res should be (Array(stripes(1)))
  }

  test("evaluate stripes for predicate state with column filters") {
    val stripes = Array(
      new StripeInformation(1.toByte, 0L, 100, Array(
        stats("a", "z", false),
        stats(1, 3, false),
        stats(1L, 3L, false)
      ), Array(
        filter("z"),
        filter(1),
        filter(2L)
      )),
      new StripeInformation(2.toByte, 101L, 100, Array(
        stats("a", "z", false),
        stats(1, 3, false),
        stats(1L, 3L, false)
      ), Array(
        filter("b"),
        filter(1),
        filter(2L)
      )))
    val state = new PredicateState(eqt("col2", "b"), td)
    val res = FileReader.evaluateStripes(stripes, state)
    // stripe 0 should be discarded because of column filter
    res should be (Array(stripes(1)))
  }

  test("file reader reuse") {
    withTempDir { dir =>
      val writer = Riff.writer(dir / "path", td)
      writer.prepareWrite()
      writer.finishWrite()

      // test prepareRead
      val reader1 = Riff.reader(dir / "path")
      reader1.prepareRead()
      var err = intercept[IOException] { reader1.prepareRead() }
      err.getMessage should be ("Reader reuse")
      err = intercept[IOException] { reader1.readFileInfo(false) }
      err.getMessage should be ("Reader reuse")

      // test readTypeDescription
      val reader2 = Riff.reader(dir / "path")
      reader2.readFileInfo(false)
      err = intercept[IOException] { reader2.readFileInfo(false) }
      err.getMessage should be ("Reader reuse")
      err = intercept[IOException] { reader2.prepareRead() }
      err.getMessage should be ("Reader reuse")
    }
  }

  test("read file header and footer") {
    withTempDir { dir =>
      val writer = Riff.writer(dir / "path", td)
      writer.prepareWrite()
      writer.finishWrite()

      val reader = Riff.reader(dir / "path")
      reader.readFileInfo(true)
      val h1 = reader.getFileHeader()
      val f1 = reader.getFileFooter()
      h1.getTypeDescription() should be (td)
      h1.state(0) should be (0)
      f1.getNumRecords() should be (0)
      f1.getFileStatistics().length should be (td.size())
    }
  }

  test("fail to get file header if it is not set") {
    withTempDir { dir =>
      touch(dir / "path")
      val reader = Riff.reader(dir / "path")
      val err = intercept[IllegalStateException] {
        reader.getFileHeader()
      }
      assert(err.getMessage.contains("File header is not set"))
    }
  }

  test("fail to get file footer if it is not set") {
    withTempDir { dir =>
      touch(dir / "path")
      val reader = Riff.reader(dir / "path")
      val err = intercept[IllegalStateException] {
        reader.getFileFooter()
      }
      assert(err.getMessage.contains("File footer is not set"))
    }
  }
}
