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

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileAlreadyExistsException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io.{CompressionCodec, ZlibCodec}
import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class FileWriterSuite extends UnitTestSuite {
  test("file writer - init without codec") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec: CompressionCodec = null
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))
      val writer = new FileWriter(fs, conf, path, td, codec)

      writer.headerPath should be (fs.makeQualified(dir / "file"))
      writer.dataPath should be (fs.makeQualified(dir / "file.data"))
      writer.hasWrittenData should be (false)
      writer.numRowsInStripe should be (Riff.Options.STRIPE_ROWS_DEFAULT)
      writer.bufferSize should be (Riff.Options.BUFFER_SIZE_DEFAULT)
      writer.toString should be (
        s"FileWriter[header=${fs.makeQualified(dir / "file")}, " +
        s"data=${fs.makeQualified(dir / "file.data")}, " +
        s"type_desc=$td, " +
        s"rows_per_stripe=${Riff.Options.STRIPE_ROWS_DEFAULT}, " +
        s"is_compressed=${codec != null}, " +
        s"buffer_size=${Riff.Options.BUFFER_SIZE_DEFAULT}]")
    }
  }

  test("file writer - init with codec") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec = new ZlibCodec()
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))
      val writer = new FileWriter(fs, conf, path, td, codec)

      writer.headerPath should be (fs.makeQualified(dir / "file"))
      writer.dataPath should be (fs.makeQualified(dir / "file.data"))
      writer.hasWrittenData should be (false)
      writer.numRowsInStripe should be (Riff.Options.STRIPE_ROWS_DEFAULT)
      writer.bufferSize should be (Riff.Options.BUFFER_SIZE_DEFAULT)
      writer.toString should be (
        s"FileWriter[header=${fs.makeQualified(dir / "file")}, " +
        s"data=${fs.makeQualified(dir / "file.data")}, " +
        s"type_desc=$td, " +
        s"rows_per_stripe=${Riff.Options.STRIPE_ROWS_DEFAULT}, " +
        s"is_compressed=${codec != null}, " +
        s"buffer_size=${Riff.Options.BUFFER_SIZE_DEFAULT}]")
    }
  }

  test("file writer - init, header path exists") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec: CompressionCodec = null
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))

      touch(path)
      intercept[FileAlreadyExistsException] {
        new FileWriter(fs, conf, path, td, codec)
      }
    }
  }

  test("file writer - init, data path exists") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec: CompressionCodec = null
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))

      touch(path.suffix(".data"))
      intercept[FileAlreadyExistsException] {
        new FileWriter(fs, conf, path, td, codec)
      }
    }
  }

  test("fail if stripe rows is negative") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.setInt(Riff.Options.STRIPE_ROWS, -1)
      val path = dir / "file"
      val codec: CompressionCodec = null
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))

      val err = intercept[IllegalArgumentException] {
        new FileWriter(fs, conf, path, td, codec)
      }
      err.getMessage should be ("Expected positive number of rows in stripe, found -1")
    }
  }

  test("select power of 2 buffer size") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec: CompressionCodec = null
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))

      conf.setInt(Riff.Options.BUFFER_SIZE, -1)
      var writer = new FileWriter(fs, conf, path, td, codec)
      writer.bufferSize should be (Riff.Options.BUFFER_SIZE_MIN)

      conf.setInt(Riff.Options.BUFFER_SIZE, Int.MaxValue)
      writer = new FileWriter(fs, conf, path, td, codec)
      writer.bufferSize should be (Riff.Options.BUFFER_SIZE_MAX)

      conf.setInt(Riff.Options.BUFFER_SIZE, 128 * 1024)
      writer = new FileWriter(fs, conf, path, td, codec)
      writer.bufferSize should be (128 * 1024)

      conf.setInt(Riff.Options.BUFFER_SIZE, 129 * 1024)
      writer = new FileWriter(fs, conf, path, td, codec)
      writer.bufferSize should be (256 * 1024)

      conf.setInt(Riff.Options.BUFFER_SIZE, 257 * 1024)
      writer = new FileWriter(fs, conf, path, td, codec)
      writer.bufferSize should be (Riff.Options.BUFFER_SIZE_MAX)
    }
  }

  test("fail to attempt to writing again") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec = new ZlibCodec()
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))
      val writer = new FileWriter(fs, conf, path, td, codec)

      writer.writeFile(Iterator(InternalRow(UTF8String.fromString("test"))))
      val err = intercept[IOException] {
        writer.writeFile(Iterator(InternalRow(UTF8String.fromString("test"))))
      }
      assert(err.getMessage.contains("No reuse of file writer"))
    }
  }

  test("write batch of rows in one stripe") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val path = dir / "file"
      val codec = new ZlibCodec()
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))
      val writer = new FileWriter(fs, conf, path, td, codec)

      val iter = (0 until 512).map { x => InternalRow(UTF8String.fromString(s"$x")) }.toIterator
      writer.writeFile(iter)
      val header = fs.getFileStatus(writer.headerPath)
      val data = fs.getFileStatus(writer.dataPath)
      // should be greater than header size
      assert(header.getLen > 16)
      assert(data.getLen > 16)
    }
  }

  test("write batch of rows in many stripes") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.setInt(Riff.Options.STRIPE_ROWS, 100)
      val path = dir / "file"
      val codec = new ZlibCodec()
      val td = new TypeDescription(StructType(StructField("col", StringType) :: Nil))
      val writer = new FileWriter(fs, conf, path, td, codec)

      val iter = (0 until 512).map { x => InternalRow(UTF8String.fromString(s"$x")) }.toIterator
      writer.writeFile(iter)
      val header = fs.getFileStatus(writer.headerPath)
      val data = fs.getFileStatus(writer.dataPath)
      // should be greater than header size
      assert(header.getLen > 16)
      assert(data.getLen > 16)
    }
  }
}
