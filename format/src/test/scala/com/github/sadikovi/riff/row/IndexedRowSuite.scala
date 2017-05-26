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

package com.github.sadikovi.riff.row

import com.github.sadikovi.testutil.UnitTestSuite

// When adding tests for read/write consider adding them into [[IndexedRowReadWriteSuite]]
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
}
