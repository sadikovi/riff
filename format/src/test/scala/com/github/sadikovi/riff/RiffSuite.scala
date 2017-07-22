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
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT)

    conf = new Configuration()
    conf.setInt(Riff.Options.HDFS_BUFFER_SIZE, Riff.Options.HDFS_BUFFER_SIZE_DEFAULT + 4096)
    Riff.Options.hdfsBufferSize(conf) should be (Riff.Options.HDFS_BUFFER_SIZE_DEFAULT + 4096)
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

  test("select column filter enabled") {
    val conf = new Configuration()
    Riff.Options.columnFilterEnabled(conf) should be (Riff.Options.COLUMN_FILTER_ENABLED_DEFAULT)

    conf.setBoolean(Riff.Options.COLUMN_FILTER_ENABLED, true)
    Riff.Options.columnFilterEnabled(conf) should be (true)
  }

  test("set conf should include previously set options") {
    withTempDir { dir =>
      val td = new TypeDescription(StructType(StructField("a", IntegerType) :: Nil))
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.setInt(Riff.Options.STRIPE_ROWS, 128)
      conf.setInt(Riff.Options.BUFFER_SIZE, 4 * 1024)
      val writer = Riff.writer(conf, dir / "path", td)
      writer.numRowsInStripe should be (128)
      writer.bufferSize should be (4 * 1024)
      writer.codec.isInstanceOf[GzipCodec] should be (true)
    }
  }

  test("set different codecs") {
    val td = new TypeDescription(StructType(StructField("a", IntegerType) :: Nil))

    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "deflate")
      val writer = Riff.writer(conf, dir / "path", td)
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      val writer = Riff.writer(conf, dir / "path", td)
      assert(writer.codec.isInstanceOf[GzipCodec])
    }

    // use compression codec from configuration, do not infer from path
    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "deflate")
      val writer = Riff.writer(conf, dir / "path.gz", td)
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    // use compression codec from configuration, do not infer from path
    withTempDir { dir =>
      val conf = new Configuration()
      conf.set(Riff.Options.COMPRESSION_CODEC, "none")
      val writer = Riff.writer(conf, dir / "path.deflate", td)
      assert(writer.codec == null)
    }
  }

  test("infer codec from path") {
    val td = new TypeDescription(StructType(StructField("a", IntegerType) :: Nil))

    withTempDir { dir =>
      val conf = new Configuration()
      val writer = Riff.writer(conf, dir / "path.gz", td)
      assert(writer.codec.isInstanceOf[GzipCodec])
    }

    withTempDir { dir =>
      val conf = new Configuration()
      val writer = Riff.writer(conf, dir / "path.deflate", td)
      assert(writer.codec.isInstanceOf[ZlibCodec])
    }

    withTempDir { dir =>
      val conf = new Configuration()
      val writer = Riff.writer(conf, dir / "path.snappy", td)
      assert(writer.codec.isInstanceOf[SnappyCodec])
    }

    // this case should have uncompressed codec, since ".deflate" is treated as filename
    withTempDir { dir =>
      val conf = new Configuration()
      val writer = Riff.writer(conf, dir / ".deflate", td)
      assert(writer.codec == null)
    }
  }

  test("create file reader") {
    withTempDir { dir =>
      touch(dir / "file")
      val reader = Riff.reader(dir / "file")
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
      codec: String,
      path: Path,
      filter: Tree = null): Seq[InternalRow] = {
    val conf = new Configuration()
    conf.set(Riff.Options.COMPRESSION_CODEC, codec)
    val td = new TypeDescription(schema, Array("col2"))
    val writer = Riff.writer(conf, path, td)
    writer.prepareWrite()
    for (row <- batch) {
      writer.write(row)
    }
    writer.finishWrite()

    val reader = Riff.reader(conf, path)
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
      // disable column filters for this write
      val conf = new Configuration(false)
      conf.set(Riff.Options.COLUMN_FILTER_ENABLED, "false")
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.setInt(Riff.Options.STRIPE_ROWS, 1)
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead().asInstanceOf[Buffers.InternalRowBuffer]
      // previous 248 bytes are 8-byte aligned
      rowbuf.offset() should be (248)
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

  test("write/read, check statistics and stripe information") {
    withTempDir { dir =>
      import RiffTestUtils._
      val conf = new Configuration(false)
      conf.setInt(Riff.Options.STRIPE_ROWS, 2)
      val td = new TypeDescription(schema, Array("col2", "col1"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead().asInstanceOf[Buffers.InternalRowBuffer]
      val header = reader.getFileHeader()
      val footer = reader.getFileFooter()
      // statistics are stored the same way type description keeps fields
      // col2, col1, col3
      val stats1 = footer.getFileStatistics()(0)
      stats1 should be (stats("abc", "xyz", false))
      val stats2 = footer.getFileStatistics()(1)
      stats2 should be (stats(1, 5, false))
      val stats3 = footer.getFileStatistics()(2)
      stats3 should be (stats(1L, 5L, false))

      rowbuf.getStripes().length should be (3)

      val stripe1 = rowbuf.getStripes()(0)
      stripe1.getStatistics() should be (
        Array(stats("abc", "def", false), stats(1, 2, false), stats(1L, 2L, false)))
      // column filters for col2
      stripe1.getColumnFilters()(0).hasNulls should be (false)
      stripe1.getColumnFilters()(0).mightContain(UTF8String.fromString("abc")) should be (true)
      stripe1.getColumnFilters()(0).mightContain(UTF8String.fromString("def")) should be (true)
      assert(stripe1.getColumnFilters()(0).toString.contains("UTF8ColumnFilter"))
      // column filters for col1
      stripe1.getColumnFilters()(1).hasNulls should be (false)
      stripe1.getColumnFilters()(1).mightContain(1) should be (true)
      stripe1.getColumnFilters()(1).mightContain(2) should be (true)
      assert(stripe1.getColumnFilters()(1).toString.contains("IntColumnFilter"))
      // column filters for col3 (NoopColumnFilter)
      stripe1.getColumnFilters()(2).hasNulls should be (false)
      assert(stripe1.getColumnFilters()(2).toString.contains("NoopColumnFilter"))

      val stripe2 = rowbuf.getStripes()(1)
      stripe2.getStatistics() should be (
        Array(stats("abc", "xyz", false), stats(3, 4, false), stats(3L, 4L, false)))
      // column filters for col2
      stripe2.getColumnFilters()(0).hasNulls should be (false)
      stripe2.getColumnFilters()(0).mightContain(UTF8String.fromString("abc")) should be (true)
      stripe2.getColumnFilters()(0).mightContain(UTF8String.fromString("xyz")) should be (true)
      assert(stripe2.getColumnFilters()(0).toString.contains("UTF8ColumnFilter"))
      // column filters for col1
      stripe2.getColumnFilters()(1).hasNulls should be (false)
      stripe2.getColumnFilters()(1).mightContain(3) should be (true)
      stripe2.getColumnFilters()(1).mightContain(4) should be (true)
      assert(stripe2.getColumnFilters()(1).toString.contains("IntColumnFilter"))
      // column filters for col3 (NoopColumnFilter)
      stripe2.getColumnFilters()(2).hasNulls should be (false)
      assert(stripe2.getColumnFilters()(2).toString.contains("NoopColumnFilter"))

      val stripe3 = rowbuf.getStripes()(2)
      stripe3.getStatistics() should be (
        Array(stats("xyz", "xyz", false), stats(5, 5, false), stats(5L, 5L, false)))
      // column filters for col2
      stripe3.getColumnFilters()(0).hasNulls should be (false)
      stripe3.getColumnFilters()(0).mightContain(UTF8String.fromString("abc")) should be (true)
      stripe3.getColumnFilters()(0).mightContain(UTF8String.fromString("xyz")) should be (true)
      assert(stripe3.getColumnFilters()(0).toString.contains("UTF8ColumnFilter"))
      // column filters for col1
      stripe3.getColumnFilters()(1).hasNulls should be (false)
      stripe3.getColumnFilters()(1).mightContain(3) should be (true)
      stripe3.getColumnFilters()(1).mightContain(4) should be (true)
      assert(stripe3.getColumnFilters()(1).toString.contains("IntColumnFilter"))
      // column filters for col3 (NoopColumnFilter)
      stripe3.getColumnFilters()(2).hasNulls should be (false)
      assert(stripe3.getColumnFilters()(2).toString.contains("NoopColumnFilter"))
    }
  }

  test("write/read, with gzip, check file properties") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)

      writer.setFileProperty("key1", "value1")
      writer.setFileProperty("key2", "value2")
      writer.setFileProperty("", "value3")
      writer.setFileProperty("key3", "")

      writer.prepareWrite()
      writer.finishWrite()

      val reader = Riff.reader(dir / "file")
      reader.readFileInfo(true)

      reader.getFileProperty("key1") should be ("value1")
      reader.getFileProperty("key2") should be ("value2")
      reader.getFileProperty("") should be ("value3")
      reader.getFileProperty("key3") should be ("")
      reader.getFileProperty("key999") should be (null)
    }
  }

  // == direct scan ==

  test("write/read with snappy, direct scan") {
    withTempDir { dir =>
      val res = writeReadTest("snappy", dir / "file")
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

  test("write/read with gzip, direct scan") {
    withTempDir { dir =>
      val res = writeReadTest("gzip", dir / "file")
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
      val res = writeReadTest("deflate", dir / "file")
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
      val res = writeReadTest("none", dir / "file")
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

  test("write/read with snappy, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest("snappy", dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read with gzip, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest("gzip", dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read with deflate, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest("deflate", dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read with no compression, filter scan") {
    withTempDir { dir =>
      val filter = eqt("col2", "def")
      val res = writeReadTest("none", dir / "file", filter)
      res.length should be (1)
      res(0).getUTF8String(0) should be (UTF8String.fromString("def"))
      res(0).getInt(1) should be (2)
      res(0).getLong(2) should be (2L)
    }
  }

  test("write/read, skip stripes because of statistics") {
    withTempDir { dir =>
      // disable column filters for this write
      val conf = new Configuration(false)
      conf.set(Riff.Options.COLUMN_FILTER_ENABLED, "false")
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.setInt(Riff.Options.STRIPE_ROWS, 1)
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead(eqt("col2", "def")).asInstanceOf[Buffers.InternalRowBuffer]
      rowbuf.getStripes().length should be (1)
      rowbuf.getStripes()(0).id() should be (1)
      // stripe has a relative offset
      // previous 248 bytes are 8-byte aligned
      rowbuf.offset() should be (248)
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
      val conf = new Configuration(false)
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.setInt(Riff.Options.STRIPE_ROWS, 1)
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      // this should skip based on header only
      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead(eqt("col2", "<none>"))
      rowbuf.isInstanceOf[Buffers.EmptyRowBuffer] should be (true)
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read with column filters") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.set(Riff.Options.COLUMN_FILTER_ENABLED, "true")
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.setInt(Riff.Options.STRIPE_ROWS, 2)
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      for (row <- batch) {
        writer.write(row)
      }
      writer.finishWrite()

      val reader = Riff.reader(dir / "file")
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
      val conf = new Configuration(false)
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, gzip, column filters enabled") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.set(Riff.Options.COMPRESSION_CODEC, "gzip")
      conf.set(Riff.Options.COLUMN_FILTER_ENABLED, "true")
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, deflate") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.set(Riff.Options.COMPRESSION_CODEC, "deflate")
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }

  test("write/read empty file, no compression") {
    withTempDir { dir =>
      val conf = new Configuration(false)
      conf.set(Riff.Options.COMPRESSION_CODEC, "none")
      val td = new TypeDescription(schema, Array("col2"))
      val writer = Riff.writer(conf, dir / "file", td)
      writer.prepareWrite()
      writer.finishWrite()
      val reader = Riff.reader(dir / "file")
      val rowbuf = reader.prepareRead()
      rowbuf.hasNext should be (false)
    }
  }
}
