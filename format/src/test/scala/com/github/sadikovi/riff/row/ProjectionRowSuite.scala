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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.testutil.UnitTestSuite

class ProjectionRowSuite extends UnitTestSuite {
  test("initialize with array of values") {
    val values = Array(1, 2, 3).map(_.asInstanceOf[AnyRef])
    val row = new ProjectionRow(values)
    row.numFields should be (3)
    row.anyNull should be (false)
    row.toString should be ("[1, 2, 3]")
  }

  test("initialize row with size") {
    val row = new ProjectionRow(3)
    row.numFields should be (3)
    row.anyNull should be (true)
    row.toString should be ("[null, null, null]")
  }

  test("initialize empty row") {
    val row = new ProjectionRow(0)
    row.numFields should be (0)
    row.anyNull should be (false)
    row.toString should be ("[empty row]")
  }

  test("update values") {
    val row = new ProjectionRow(3)
    row.numFields should be (3)
    row.anyNull should be (true)
    row.toString should be ("[null, null, null]")

    row.update(0, 1)
    row.update(1, 2)
    row.update(2, 3)
    row.anyNull should be (false)
    row.toString should be ("[1, 2, 3]")
  }

  test("get int value") {
    val row = new ProjectionRow(1)
    row.update(0, 12345)
    row.getInt(0) should be (12345)
    row.get(0, IntegerType) should be (12345)
  }

  test("get long value") {
    val row = new ProjectionRow(1)
    row.update(0, -9999L)
    row.getLong(0) should be (-9999L)
    row.get(0, LongType) should be (-9999L)
  }

  test("get string value") {
    val row = new ProjectionRow(1)
    row.update(0, UTF8String.fromString("abcdef"))
    row.getUTF8String(0) should be (UTF8String.fromString("abcdef"))
    row.get(0, StringType) should be (UTF8String.fromString("abcdef"))
  }

  test("get date value") {
    val row = new ProjectionRow(1)
    // date is stored internally as integer
    row.update(0, 1230000)
    row.getInt(0) should be (1230000)
    row.get(0, DateType) should be (1230000)
  }

  test("get timestamp value") {
    val row = new ProjectionRow(1)
    // timestamp is stored internally as long
    row.update(0, 4567890L)
    row.getLong(0) should be (4567890L)
    row.get(0, TimestampType) should be (4567890L)
  }

  test("get boolean value") {
    val row = new ProjectionRow(2)
    row.update(0, true)
    row.update(1, false)
    row.getBoolean(0) should be (true)
    row.getBoolean(1) should be (false)
    assert(row.get(0, BooleanType) === true)
    assert(row.get(1, BooleanType) === false)
  }

  test("get short value") {
    val row = new ProjectionRow(1)
    row.update(0, 763.toShort)
    row.getShort(0) should be (763.toShort)
    row.get(0, ShortType) should be (763.toShort)
  }

  test("get byte value") {
    val row = new ProjectionRow(1)
    row.update(0, 68.toByte)
    row.getByte(0) should be (68.toByte)
    row.get(0, ByteType) should be (68.toByte)
  }
}
