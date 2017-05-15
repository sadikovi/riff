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

import java.io.ByteArrayInputStream

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io.{OutStream, StripeOutputBuffer}
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class BufferSuite extends UnitTestSuite {
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

  val td = new TypeDescription(schema, Array("col2"))

  test("prepare row buffer - select empty row buffer for null stripes") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val rowbuf = Buffers.prepareRowBuffer(in, null, td, null, 128, null)
      rowbuf.toString should be ("EmptyRowBuffer")
    }
  }

  test("prepare row buffer - select empty row buffer for empty stripes") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val rowbuf = Buffers.prepareRowBuffer(in, Array.empty, td, null, 128, null)
      rowbuf.toString should be ("EmptyRowBuffer")
    }
  }

  test("prepare row buffer - select direct scan for null predicate state") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val rowbuf = Buffers.prepareRowBuffer(in,
        Array(new StripeInformation(1.toShort, 0L, 64, null)), td, null, 16, null)
      rowbuf.toString should be ("DirectScanRowBuffer")
    }
  }

  test("prepare row buffer - select direct scan for trivial positive predicate state") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val state = new PredicateState(or(TRUE, eqt("col1", 1)), td)
      val rowbuf = Buffers.prepareRowBuffer(in,
        Array(new StripeInformation(1.toShort, 0L, 64, null)), td, null, 16, state)
      rowbuf.toString should be ("DirectScanRowBuffer")
    }
  }

  test("prepare row buffer - select empty scan for trivial negative predicate state") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val state = new PredicateState(and(FALSE, eqt("col1", 1)), td)
      val rowbuf = Buffers.prepareRowBuffer(in,
        Array(new StripeInformation(1.toShort, 0L, 64, null)), td, null, 16, state)
      rowbuf.toString should be ("EmptyRowBuffer")
    }
  }

  test("select empty row buffer") {
    withTempDir { dir =>
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val rowbuf = Buffers.prepareRowBuffer(in, null, td, null, 16, null)
      rowbuf.hasNext should be (false)
      val err = intercept[NoSuchElementException] {
        rowbuf.next
      }
      err.getMessage should be ("Empty iterator")
      rowbuf.close()
    }
  }

  test("initialize empty row buffer with null stream") {
    val rowbuf = Buffers.emptyRowBuffer(null)
    rowbuf.hasNext should be (false)
    val err = intercept[NoSuchElementException] {
      rowbuf.next
    }
    err.getMessage should be ("Empty iterator")
    // should be able to close with null stream
    rowbuf.close()
  }

  test("select direct scan buffer") {
    withTempDir { dir =>
      val writer = new IndexedRowWriter(td)
      val stripe = new StripeOutputBuffer(1.toByte)
      val out = new OutStream(8, null, stripe)
      for (row <- batch) {
        writer.writeRow(row, out)
      }
      out.flush()
      // create file and write data
      val outStream = create(dir / "file")
      outStream.write(stripe.array)
      outStream.close()

      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val stripes = Array(new StripeInformation(1.toByte, 0, stripe.array.length, null))
      val rowbuf = Buffers.prepareRowBuffer(in, stripes, td, null, 16, null)
      var seq = Seq[InternalRow]()
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      seq.length should be (batch.length)
    }
  }

  test("select direct scan buffer on empty stream") {
    withTempDir { dir =>
      // create empty file - this represents stripe bytes, not total data file bytes, because that
      // would include file header + metadata + statistics
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val stripes = Array(new StripeInformation(1.toByte, 0, 0, null))
      val rowbuf = Buffers.prepareRowBuffer(in, stripes, td, null, 16, null)
      var seq = Seq[InternalRow]()
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      seq.length should be (0)
    }
  }

  test("select predicate scan buffer") {
    withTempDir { dir =>
      val writer = new IndexedRowWriter(td)
      val stripe = new StripeOutputBuffer(1.toByte)
      val out = new OutStream(8, null, stripe)
      for (row <- batch) {
        writer.writeRow(row, out)
      }
      out.flush()
      // create file and write data
      val outStream = create(dir / "file")
      outStream.write(stripe.array)
      outStream.close()

      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val stripes = Array(new StripeInformation(1.toByte, 0, stripe.array.length, null))
      val state = new PredicateState(or(eqt("col1", 1), eqt("col3", 4L)), td)
      val rowbuf = Buffers.prepareRowBuffer(in, stripes, td, null, 16, state)
      var seq = Seq[InternalRow]()
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      seq.length should be (2)
    }
  }

  test("select predicate scan buffer with filter that selects no records") {
    withTempDir { dir =>
      val writer = new IndexedRowWriter(td)
      val stripe = new StripeOutputBuffer(1.toByte)
      val out = new OutStream(8, null, stripe)
      for (row <- batch) {
        writer.writeRow(row, out)
      }
      out.flush()
      // create file and write data
      val outStream = create(dir / "file")
      outStream.write(stripe.array)
      outStream.close()

      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val stripes = Array(new StripeInformation(1.toByte, 0, stripe.array.length, null))
      val state = new PredicateState(lt("col1", 1), td)
      val rowbuf = Buffers.prepareRowBuffer(in, stripes, td, null, 16, state)
      var seq = Seq[InternalRow]()
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      seq.length should be (0)
    }
  }

  test("select predicate scan buffer on empty stream") {
    withTempDir { dir =>
      // create empty file - this represents stripe bytes, not total data file bytes, because that
      // would include file header + metadata + statistics
      touch(dir / "file")
      val in = open(dir / "file").asInstanceOf[FSDataInputStream]
      val stripes = Array(new StripeInformation(1.toByte, 0, 0, null))
      val state = new PredicateState(lt("col1", 1), td)
      val rowbuf = Buffers.prepareRowBuffer(in, stripes, td, null, 16, state)
      var seq = Seq[InternalRow]()
      while (rowbuf.hasNext) {
        seq = seq :+ rowbuf.next
      }
      seq.length should be (0)
    }
  }
}
