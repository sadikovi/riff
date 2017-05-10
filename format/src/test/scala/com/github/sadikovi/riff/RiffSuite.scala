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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io._
import com.github.sadikovi.riff.tree.Tree
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class RiffSuite extends UnitTestSuite {
  test("select compression codec in configuration") {
    var conf = new Configuration()
    Riff.Options.compressionCodecName(conf) should be (null)
    conf = new Configuration()
    conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
    Riff.Options.compressionCodecName(conf) should be ("gzip")
  }

  test("select buffer size") {
    var conf = new Configuration()
    conf.setInt(Riff.Options.BUFFER_SIZE, -1)
    Riff.Options.power2BufferSize(conf) should be (Riff.Options.BUFFER_SIZE_MIN)

    conf = new Configuration()
    conf.setInt(Riff.Options.BUFFER_SIZE, Int.MaxValue)
    Riff.Options.power2BufferSize(conf) should be (Riff.Options.BUFFER_SIZE_MAX)

    conf = new Configuration()
    conf.setInt(Riff.Options.BUFFER_SIZE, 128 * 1024)
    Riff.Options.power2BufferSize(conf) should be (128 * 1024)

    conf = new Configuration()
    conf.setInt(Riff.Options.BUFFER_SIZE, 129 * 1024)
    Riff.Options.power2BufferSize(conf) should be (256 * 1024)

    conf = new Configuration()
    conf.setInt(Riff.Options.BUFFER_SIZE, 257 * 1024)
    Riff.Options.power2BufferSize(conf) should be (Riff.Options.BUFFER_SIZE_MAX)
  }

  test("select hdfs buffer size") {
    var conf = new Configuration()
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.HDFS_BUFFER_SIZE, 0)
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.HDFS_BUFFER_SIZE, -1)
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.HDFS_BUFFER_SIZE, Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.HDFS_BUFFER_SIZE, Riff.Options.HDFS_BUFFER_SIZE_DEFAULT + 1)
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT * 2)
  }

  test("num rows in stripe") {
    var conf = new Configuration()
    Riff.Options.numRowsInStripe(conf) should be (Riff.Options.STRIPE_ROWS_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.STRIPE_ROWS, 1)
    Riff.Options.numRowsInStripe(conf) should be (1)

    conf = new Configuration()
    conf.setInt(Riff.Options.STRIPE_ROWS, Riff.Options.STRIPE_ROWS_DEFAULT)
    Riff.Options.numRowsInStripe(conf) should be (Riff.Options.STRIPE_ROWS_DEFAULT)

    var err = intercept[IllegalArgumentException] {
      conf = new Configuration()
      conf.setInt(Riff.Options.STRIPE_ROWS, 0)
      Riff.Options.numRowsInStripe(conf)
    }
    assert(err.getMessage.contains("Expected positive number of rows in stripe"))

    err = intercept[IllegalArgumentException] {
      conf = new Configuration()
      conf.setInt(Riff.Options.STRIPE_ROWS, -1)
      Riff.Options.numRowsInStripe(conf)
    }
    assert(err.getMessage.contains("Expected positive number of rows in stripe"))
  }

  test("make data path") {
    val path = new Path("/a/b/c")
    val dpath = Riff.makeDataPath(path)
    dpath should be (new Path("/a/b/.c.data"))
  }

  test("select column filter enabled") {
    val conf = new Configuration()
    Riff.Options.columnFilterEnabled(conf) should be (Riff.Options.COLUMN_FILTER_ENABLED_DEFAULT)

    conf.setBoolean(Riff.Options.COLUMN_FILTER_ENABLED, true)
    Riff.Options.columnFilterEnabled(conf) should be (true)
  }

  test("fail writer if type description is not set") {
    withTempDir { dir =>
      val err = intercept[RuntimeException] {
        Riff.writer.create(new Path(s"file:$dir"))
      }
      err.getMessage should be ("Type description is not set")
    }
  }

  test("set codec manually") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .setCodec(new ZlibCodec())
        .create(dir / "path")
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .setCodec("gzip").create(dir / "path")
      assert(writer.codec.isInstanceOf[GzipCodec])
    }

    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .setCodec("deflate")
        .create(dir / "path.gz")
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .setCodec("none")
        .create(dir / "path.deflate")
      assert(writer.codec == null)
    }
  }

  test("infer codec from path") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / "path.gz")
      assert(writer.codec.isInstanceOf[GzipCodec])
    }

    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / "path.deflate")
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    // this case should have uncompressed codec, since ".deflate" is treated as filename
    withTempDir { dir =>
      val writer = Riff.writer
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / ".deflate")
      assert(writer.codec == null)
    }
  }

  test("read codec from configuration") {
    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "none")
      val writer = Riff.writer
        .setConf(conf)
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / "path.gz")
      assert(writer.codec == null)
    }

    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      val writer = Riff.writer
        .setConf(conf)
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / "path.deflate")
      assert(writer.codec.isInstanceOf[GzipCodec])
    }

    // even though ".deflate" is treated as filename we force deflate compression through conf
    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "deflate")
      val writer = Riff.writer
        .setConf(conf)
        .setTypeDesc(StructType(StructField("a", IntegerType) :: Nil))
        .create(dir / ".deflate")
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }
  }

  test("create file reader") {
    withTempDir { dir =>
      val reader = Riff.reader.create(dir / "file")
      assert(reader.filePath.toString == s"file:${dir / "file"}")
    }
  }

  test("create file reader using path as string") {
    withTempDir { dir =>
      val reader = Riff.reader.create(s"${dir / "file"}")
      assert(reader.filePath.toString == s"file:${dir / "file"}")
    }
  }

  //////////////////////////////////////////////////////////////
  // == Read/write tests ==
  //////////////////////////////////////////////////////////////

  val schema = StructType(
    StructField("col1", IntegerType) ::
    StructField("col2", StringType) ::
    StructField("col3", LongType) :: Nil)

  val batch = Seq(
    InternalRow(1, UTF8String.fromString("abc"), 1L),
    InternalRow(2, UTF8String.fromString("def"), 2L),
    InternalRow(3, UTF8String.fromString("abc"), 3L),
    InternalRow(4, UTF8String.fromString("xyz"), 4L),
    InternalRow(5, UTF8String.fromString("xyz"), 5L))

  def writeReadTest(
      codec: CompressionCodec,
      path: Path,
      filter: Tree = null): Seq[InternalRow] = {
    val writer = Riff.writer.setCodec(codec).setTypeDesc(schema, "col2").create(path)
    writer.prepareWrite()
    for (row <- batch) {
      writer.write(row)
    }
    writer.finishWrite()

    val reader = Riff.reader.create(path)
    val rowbuf = reader.prepareRead(filter)
    var seq: Seq[InternalRow] = Nil
    while (rowbuf.hasNext) {
      seq = seq :+ rowbuf.next
    }
    rowbuf.close()
    seq
  }

  test("write/read, with gzip, check stripe info") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setCodec("gzip")
        .setRowsInStripe(1)
        .setTypeDesc(schema, "col2")
        .create(dir / "file")
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead().asInstanceOf[Buffers.InternalRowBuffer]
      rowbuf.offset() should be (612)
      rowbuf.getStripes().length should be (5)

      rowbuf.getStripes()(0).offset() should be (0)
      rowbuf.getStripes()(0).length() should be (36)
      rowbuf.getStripes()(0).hasStatistics() should be (true)

      rowbuf.getStripes()(1).offset() should be (36)
      rowbuf.getStripes()(1).length() should be (36)
      rowbuf.getStripes()(1).hasStatistics() should be (true)

      rowbuf.getStripes()(2).offset() should be (72)
      rowbuf.getStripes()(2).length() should be (36)
      rowbuf.getStripes()(2).hasStatistics() should be (true)

      rowbuf.getStripes()(3).offset() should be (108)
      rowbuf.getStripes()(3).length() should be (36)
      rowbuf.getStripes()(3).hasStatistics() should be (true)

      rowbuf.getStripes()(4).offset() should be (144)
      rowbuf.getStripes()(4).length() should be (36)
      rowbuf.getStripes()(4).hasStatistics() should be (true)

      rowbuf.close()
    }
  }

  // == direct scan ==

  test("write/read with gzip, direct scan") {
    withTempDir { dir =>
      val res = writeReadTest(new GzipCodec(), dir / "file")
      res.length should be (batch.length)
      for (i <- 0 until res.length) {
        val row1 = res(i)
        val row2 = batch(i)
        row1.getUTF8String(0) should be (row2.getUTF8String(1))
        row1.getInt(1) should be (row2.getInt(0))
        row1.getLong(2) should be (row2.getLong(2))
      }
    }
  }

  test("write/read with deflate, direct scan") {
    withTempDir { dir =>
      val res = writeReadTest(new ZlibCodec(), dir / "file")
      res.length should be (batch.length)
      for (i <- 0 until res.length) {
        val row1 = res(i)
        val row2 = batch(i)
        row1.getUTF8String(0) should be (row2.getUTF8String(1))
        row1.getInt(1) should be (row2.getInt(0))
        row1.getLong(2) should be (row2.getLong(2))
      }
    }
  }

  test("write/read with no compression, direct scan") {
    withTempDir { dir =>
      val res = writeReadTest(null, dir / "file")
      res.length should be (batch.length)
      for (i <- 0 until res.length) {
        val row1 = res(i)
        val row2 = batch(i)
        row1.getUTF8String(0) should be (row2.getUTF8String(1))
        row1.getInt(1) should be (row2.getInt(0))
        row1.getLong(2) should be (row2.getLong(2))
      }
    }
  }

  // == filter scan ==

  test("write/read with gzip, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest(new GzipCodec(), dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read with deflate, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest(new ZlibCodec(), dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read with no compression, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest(null, dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read, skip stripes because of statistics") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setCodec("gzip")
        .setRowsInStripe(1)
        .setTypeDesc(schema, "col2")
        .create(dir / "file")
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead(eqt("col2", "def")).asInstanceOf[Buffers.InternalRowBuffer]
      rowbuf.getStripes().length should be (1)
      rowbuf.getStripes()(0).id() should be (1)
      // stripe has a relative offset
      rowbuf.offset() should be (612)
      rowbuf.getStripes()(0).offset() should be (36)
      rowbuf.getStripes()(0).length() should be (36)
      rowbuf.getStripes()(0).hasStatistics() should be (true)

      var seq: Seq[InternalRow] = Nil
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      rowbuf.close()

      seq.length should be (1)
      seq(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      seq(0).getInt(1) should be (2)
      seq(0).getLong(2) should be (2L)
    }
  }

  test("write/read, skip file because of statistics") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setCodec("gzip")
        .setRowsInStripe(1)
        .setTypeDesc(schema, "col2")
        .create(dir / "file")
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      // this should skip based on header only
      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead(eqt("col2", "<none>"))
      rowbuf.isInstanceOf[Buffers.EmptyRowBuffer] should be (true)
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read with column filters") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setCodec("gzip")
        .setRowsInStripe(2)
        .enableColumnFilter(true)
        .setTypeDesc(schema, "col2")
        .create(dir / "file")
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead()
      var cnt = 0
      while (rowbuf.hasNext) {
        rowbuf.next
        cnt += 1
      }
      cnt should be (batch.length)
    }
  }

  // == empty file scan ==

  test("write/read empty file, gzip") {
    withTempDir { dir =>
      val writer = Riff.writer.setCodec("gzip").setTypeDesc(schema, "col2").create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, gzip, column filters enabled") {
    withTempDir { dir =>
      val writer = Riff.writer
        .setCodec("gzip")
        .setTypeDesc(schema, "col2")
        .enableColumnFilter(true).create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, deflate") {
    withTempDir { dir =>
      val writer = Riff.writer.setCodec("deflate").setTypeDesc(schema, "col2").create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, no compression") {
    withTempDir { dir =>
      val writer = Riff.writer.setCodec("none").setTypeDesc(schema, "col2").create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader.create(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }
}
