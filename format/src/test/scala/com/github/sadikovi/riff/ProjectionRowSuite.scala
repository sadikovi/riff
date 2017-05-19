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

  test("get values") {
    val row = new ProjectionRow(5)
    row.update(0, 1)
    row.update(1, 2L)
    row.update(2, UTF8String.fromString("abc"))
    row.update(3, true)
    row.update(4, false)

    row.getInt(0) should be (1)
    row.getLong(1) should be (2L)
    row.getUTF8String(2) should be (UTF8String.fromString("abc"))

    row.get(0, IntegerType) should be (1)
    row.get(1, LongType) should be (2L)
    row.get(2, StringType) should be (UTF8String.fromString("abc"))
    row.get(0, DateType) should be (1)
    row.get(1, TimestampType) should be (2L)
    assert(row.get(3, BooleanType) === true)
    assert(row.get(4, BooleanType) === false)
  }
}
