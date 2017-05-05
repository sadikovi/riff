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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.io._
import com.github.sadikovi.riff.tree.Tree
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class IndexedRowSuite extends UnitTestSuite {
  test("indexed row - too many fields") {
    val err = intercept[IllegalArgumentException] {
      new IndexedRow(0L, 0L, new Array[Int](65))
    }
    err.getMessage should be ("Too many fields 65, should be <= 64")
  }

  test("indexed row - set/check index region") {
    val row = new IndexedRow(0L, 0L, new Array[Int](32))
    row.hasIndexRegion() should be (false)
    row.hasDataRegion() should be (false)

    row.setIndexRegion(Array[Byte](1, 2, 3, 4))
    row.hasIndexRegion() should be (true)
    row.hasDataRegion() should be (false)
  }

  test("indexed row - set/check data region") {
    val row = new IndexedRow(0L, 0L, new Array[Int](32))
    row.hasIndexRegion() should be (false)
    row.hasDataRegion() should be (false)

    row.setDataRegion(Array[Byte](1, 2, 3, 4))
    row.hasIndexRegion() should be (false)
    row.hasDataRegion() should be (true)
  }

  test("indexed row - num fields") {
    val row1 = new IndexedRow(0L, 0L, new Array[Int](32))
    row1.numFields() should be (32)
    val row2 = new IndexedRow(0L, 0L, new Array[Int](1))
    row2.numFields() should be (1)
    val row3 = new IndexedRow(0L, 0L, new Array[Int](0))
    row3.numFields() should be (0)
  }

  test("indexed row - nulls 1") {
    var nulls = 123L
    val row = new IndexedRow(0L, nulls, new Array[Int](32))
    row.getNulls() should be (nulls)
    row.anyNull() should be (true)
    var ordinal = 0
    while (nulls != 0) {
      val isNull = (nulls & 0x1) != 0
      row.isNullAt(ordinal) should be (isNull)
      nulls >>>= 1
      ordinal += 1
    }
  }

  test("indexed row - nulls 2") {
    val row = new IndexedRow(0L, 0L, new Array[Int](32))
    row.getNulls() should be (0L)
    row.anyNull() should be (false)
  }

  test("indexed row - isIndexed") {
    var indexed = 123L
    val row = new IndexedRow(indexed, 0L, new Array[Int](32))
    row.getIndexed() should be (indexed)
    var ordinal = 0
    while (indexed != 0) {
      val isIndexed = (indexed & 0x1) != 0
      row.isIndexed(ordinal) should be (isIndexed)
      indexed >>>= 1
      ordinal += 1
    }
  }

  test("indexd row - copy 1") {
    val row = new IndexedRow(123L, 124L, new Array[Int](32))
    val copy = row.copy()
    copy.getNulls() should be (row.getNulls())
    copy.getIndexed() should be (row.getIndexed())
    copy.hasIndexRegion() should be (row.hasIndexRegion())
    copy.hasDataRegion() should be (row.hasDataRegion())
  }

  test("indexd row - copy 2") {
    val row = new IndexedRow(123L, 124L, new Array[Int](32))
    row.setIndexRegion(Array[Byte](1, 2, 3, 4))
    row.setDataRegion(Array[Byte](5, 6, 7, 8))
    val copy = row.copy()
    copy.getNulls() should be (row.getNulls())
    copy.getIndexed() should be (row.getIndexed())
    copy.hasIndexRegion() should be (row.hasIndexRegion())
    copy.hasDataRegion() should be (row.hasDataRegion())
  }

  test("indexed row - copy does not share region buffers") {
    // integer value of 123
    val bytes = Array[Byte](0, 0, 0, 123)
    // row contains 2 columns, first is indexed int, second is int
    val row1 = new IndexedRow(1L, 0L, Array(0, 0))
    row1.setIndexRegion(bytes)
    row1.setDataRegion(bytes)
    val row2 = row1.copy()

    // modify content of byte array
    bytes(3) = 128.toByte

    row1.getInt(0) should be (128)
    row1.getInt(1) should be (128)
    // copy should preserve old values before update
    row2.getInt(0) should be (123)
    row2.getInt(1) should be (123)
  }

  test("indexed row - toString 1") {
    val row = new IndexedRow(123L, 124L, new Array[Int](32))
    row.setIndexRegion(Array[Byte](1, 2, 3, 4))
    row.setDataRegion(Array[Byte](5, 6, 7, 8))
    row.toString() should be (
      "[nulls=true, fields=32, index_region=[1, 2, 3, 4], data_region=[5, 6, 7, 8]]")
  }

  test("indexed row - toString 2") {
    val row = new IndexedRow(123L, 0L, new Array[Int](8))
    row.toString() should be (
      "[nulls=false, fields=8, index_region=null, data_region=null]")
  }

  test("write/read, indexed + data fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) ::
      StructField("col4", StringType) ::
      StructField("col5", IntegerType) ::
      StructField("col6", StringType) :: Nil)
    val row = InternalRow(24, UTF8String.fromString("abcd"), 123L,
      UTF8String.fromString("xyz"), 789, null)

    val td = new TypeDescription(schema, Array("col3", "col2"))
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)
    writer.writeRow(row, out)
    out.flush()
    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    val ind = reader.readRow(in).asInstanceOf[IndexedRow]

    ind.hasIndexRegion() should be (true)
    ind.hasDataRegion() should be (true)

    ind.isNullAt(0) should be (false)
    ind.getLong(0) should be (123L)
    ind.get(0, LongType) should be (123L)

    ind.isNullAt(1) should be (false)
    ind.getString(1) should be ("abcd")
    ind.get(1, StringType) should be (UTF8String.fromString("abcd"))

    ind.isNullAt(2) should be (false)
    ind.getInt(2) should be (24)
    ind.get(2, IntegerType) should be (24)

    ind.isNullAt(3) should be (false)
    ind.getString(3) should be ("xyz")
    ind.get(3, StringType) should be (UTF8String.fromString("xyz"))

    ind.isNullAt(4) should be (false)
    ind.getInt(4) should be (789)
    ind.get(4, IntegerType) should be (789)

    ind.isNullAt(5) should be (true)
  }

  test("write/read, indexed fields only") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val row = InternalRow(24, UTF8String.fromString("abcd"), 123L)

    val td = new TypeDescription(schema, Array("col3", "col2", "col1"))
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)
    writer.writeRow(row, out)
    out.flush()
    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    val ind = reader.readRow(in).asInstanceOf[IndexedRow]

    ind.hasIndexRegion() should be (true)
    ind.hasDataRegion() should be (false)

    ind.isNullAt(0) should be (false)
    ind.getLong(0) should be (123L)
    ind.get(0, LongType) should be (123L)

    ind.isNullAt(1) should be (false)
    ind.getString(1) should be ("abcd")
    ind.get(1, StringType) should be (UTF8String.fromString("abcd"))

    ind.isNullAt(2) should be (false)
    ind.getInt(2) should be (24)
    ind.get(2, IntegerType) should be (24)
  }

  test("write/read, data fields only") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val row = InternalRow(24, UTF8String.fromString("abcd"), 123L)

    val td = new TypeDescription(schema)
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)
    writer.writeRow(row, out)
    out.flush()
    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    val ind = reader.readRow(in).asInstanceOf[IndexedRow]

    ind.hasIndexRegion() should be (false)
    ind.hasDataRegion() should be (true)

    ind.isNullAt(0) should be (false)
    ind.getInt(0) should be (24)
    ind.get(0, IntegerType) should be (24)

    ind.isNullAt(1) should be (false)
    ind.getString(1) should be ("abcd")
    ind.get(1, StringType) should be (UTF8String.fromString("abcd"))

    ind.isNullAt(2) should be (false)
    ind.getLong(2) should be (123L)
    ind.get(2, LongType) should be (123L)
  }

  test("write/read, all null fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val row = InternalRow(null, null, null)

    val td = new TypeDescription(schema)
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)
    writer.writeRow(row, out)
    out.flush()
    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    val ind = reader.readRow(in).asInstanceOf[IndexedRow]

    ind.hasIndexRegion() should be (false)
    ind.hasDataRegion() should be (false)

    ind.isNullAt(0) should be (true)
    ind.isNullAt(1) should be (true)
    ind.isNullAt(2) should be (true)
  }

  test("write/read, row batch") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val batch = Seq(
      InternalRow(1, UTF8String.fromString("abc1"), 1L),
      InternalRow(2, UTF8String.fromString("abc2"), 2L),
      InternalRow(3, UTF8String.fromString("abc3"), 3L),
      InternalRow(4, UTF8String.fromString("abc4"), 4L),
      InternalRow(5, UTF8String.fromString("abc5"), 5L))

    val td = new TypeDescription(schema, Array("col2"))
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)

    for (row <- batch) {
      writer.writeRow(row, out)
    }
    out.flush()

    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    var ind = Seq[IndexedRow]()
    while (in.available() != 0) {
      ind = ind :+ reader.readRow(in).asInstanceOf[IndexedRow]
    }

    ind.length should be (batch.length)
    ind(0).toString should be (
      "[nulls=false, fields=3, " +
      "index_region=[0, 0, 0, 8, 0, 0, 0, 4, 97, 98, 99, 49], " +
      "data_region=[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]]")
    ind(1).toString should be (
      "[nulls=false, fields=3, " +
      "index_region=[0, 0, 0, 8, 0, 0, 0, 4, 97, 98, 99, 50], " +
      "data_region=[0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2]]")
    ind(2).toString should be (
      "[nulls=false, fields=3, " +
      "index_region=[0, 0, 0, 8, 0, 0, 0, 4, 97, 98, 99, 51], " +
      "data_region=[0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3]]")
    ind(3).toString should be (
      "[nulls=false, fields=3, " +
      "index_region=[0, 0, 0, 8, 0, 0, 0, 4, 97, 98, 99, 52], " +
      "data_region=[0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4]]")
    ind(4).toString should be (
      "[nulls=false, fields=3, " +
      "index_region=[0, 0, 0, 8, 0, 0, 0, 4, 97, 98, 99, 53], " +
      "data_region=[0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 5]]")
  }

  // read rows for provided filter
  // col2 - indexed field
  // col1 and col3 - data fields
  private def readWithPredicate(tree: Tree): (TypeDescription, Seq[IndexedRow]) = {
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
    val writer = new IndexedRowWriter(td)
    val reader = new IndexedRowReader(td)
    val state = new PredicateState(tree, td)
    val stripe = new StripeOutputBuffer(1.toByte)
    val out = new OutStream(64, null, stripe)

    for (row <- batch) {
      writer.writeRow(row, out)
    }
    out.flush()

    val in = new InStream(64, null, new StripeInputBuffer(1.toByte, stripe.array()))
    var ind = Seq[IndexedRow]()
    while (in.available() != 0) {
      ind = ind :+ reader.readRow(in, state).asInstanceOf[IndexedRow]
    }
    (td, ind)
  }

  test("write/read with predicate on both index and data fields") {
    val (td, ind) = readWithPredicate(or(eqt("col2", "abc"), gt("col1", 4)))
    // sequence should contain following records
    ind(0).getUTF8String(td.position("col2")) should be (UTF8String.fromString("abc"))
    ind(0).getInt(td.position("col1")) should be (1)
    ind(1) should be (null)
    ind(2).getUTF8String(td.position("col2")) should be (UTF8String.fromString("abc"))
    ind(2).getInt(td.position("col1")) should be (3)
    ind(3) should be (null)
    ind(4).getUTF8String(td.position("col2")) should be (UTF8String.fromString("xyz"))
    ind(4).getInt(td.position("col1")) should be (5)
  }

  test("write/read with predicate on data fields") {
    val (td, ind) = readWithPredicate(in("col1",
      1.asInstanceOf[java.lang.Integer], 4.asInstanceOf[java.lang.Integer]))
    // sequence should contain following records
    ind(0).getUTF8String(td.position("col2")) should be (UTF8String.fromString("abc"))
    ind(0).getInt(td.position("col1")) should be (1)
    ind(1) should be (null)
    ind(2) should be (null)
    ind(3).getUTF8String(td.position("col2")) should be (UTF8String.fromString("xyz"))
    ind(3).getInt(td.position("col1")) should be (4)
    ind(4) should be (null)
  }

  test("write/read with predicate on index fields") {
    val (td, ind) = readWithPredicate(in("col2", "def", "abc"))
    // sequence should contain following records
    ind(0).getUTF8String(td.position("col2")) should be (UTF8String.fromString("abc"))
    ind(0).getInt(td.position("col1")) should be (1)
    ind(1).getUTF8String(td.position("col2")) should be (UTF8String.fromString("def"))
    ind(1).getInt(td.position("col1")) should be (2)
    ind(2).getUTF8String(td.position("col2")) should be (UTF8String.fromString("abc"))
    ind(2).getInt(td.position("col1")) should be (3)
    ind(3) should be (null)
    ind(4) should be (null)
  }
}
