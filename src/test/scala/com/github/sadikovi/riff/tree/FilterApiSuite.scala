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

package com.github.sadikovi.riff.tree

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class FilterApiSuite extends UnitTestSuite {
  // == EqualTo ==
  test("EqualTo - int") {
    val p = eqt("col", 123)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getInt() should be (123)
    p.resolved() should be (false)
    p.toString should be ("*col = 123")
    p.withOrdinal(1).toString should be ("col[1] = 123")
  }

  test("EqualTo - long") {
    val p = eqt("col", 123L)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLong() should be (123L)
    p.resolved() should be (false)
    p.toString should be ("*col = 123L")
    p.withOrdinal(1).toString should be ("col[1] = 123L")
  }

  test("EqualTo - UTF8String") {
    val p = eqt("col", "123")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8String() should be (UTF8String.fromString("123"))
    p.resolved() should be (false)
    p.toString should be ("*col = '123'")
    p.withOrdinal(1).toString should be ("col[1] = '123'")
  }

  // == GreaterThan ==
  test("GreaterThan - int") {
    val p = gt("col", 123)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getInt() should be (123)
    p.resolved() should be (false)
    p.toString should be ("*col > 123")
    p.withOrdinal(1).toString should be ("col[1] > 123")
  }

  test("GreaterThan - long") {
    val p = gt("col", 123L)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLong() should be (123L)
    p.resolved() should be (false)
    p.toString should be ("*col > 123L")
    p.withOrdinal(1).toString should be ("col[1] > 123L")
  }

  test("GreaterThan - UTF8String") {
    val p = gt("col", "123")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8String() should be (UTF8String.fromString("123"))
    p.resolved() should be (false)
    p.toString should be ("*col > '123'")
    p.withOrdinal(1).toString should be ("col[1] > '123'")
  }

  // == LessThan ==
  test("LessThan - int") {
    val p = lt("col", 123)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getInt() should be (123)
    p.resolved() should be (false)
    p.toString should be ("*col < 123")
    p.withOrdinal(1).toString should be ("col[1] < 123")
  }

  test("LessThan - long") {
    val p = lt("col", 123L)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLong() should be (123L)
    p.resolved() should be (false)
    p.toString should be ("*col < 123L")
    p.withOrdinal(1).toString should be ("col[1] < 123L")
  }

  test("LessThan - UTF8String") {
    val p = lt("col", "123")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8String() should be (UTF8String.fromString("123"))
    p.resolved() should be (false)
    p.toString should be ("*col < '123'")
    p.withOrdinal(1).toString should be ("col[1] < '123'")
  }

  // == GreaterThanOrEqual ==
  test("GreaterThanOrEqual - int") {
    val p = ge("col", 123)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getInt() should be (123)
    p.resolved() should be (false)
    p.toString should be ("*col >= 123")
    p.withOrdinal(1).toString should be ("col[1] >= 123")
  }

  test("GreaterThanOrEqual - long") {
    val p = ge("col", 123L)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLong() should be (123L)
    p.resolved() should be (false)
    p.toString should be ("*col >= 123L")
    p.withOrdinal(1).toString should be ("col[1] >= 123L")
  }

  test("GreaterThanOrEqual - UTF8String") {
    val p = ge("col", "123")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8String() should be (UTF8String.fromString("123"))
    p.resolved() should be (false)
    p.toString should be ("*col >= '123'")
    p.withOrdinal(1).toString should be ("col[1] >= '123'")
  }

  // == LessThanOrEqual ==
  test("LessThanOrEqual - int") {
    val p = le("col", 123)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getInt() should be (123)
    p.resolved() should be (false)
    p.toString should be ("*col <= 123")
    p.withOrdinal(1).toString should be ("col[1] <= 123")
  }

  test("LessThanOrEqual - long") {
    val p = le("col", 123L)
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLong() should be (123L)
    p.resolved() should be (false)
    p.toString should be ("*col <= 123L")
    p.withOrdinal(1).toString should be ("col[1] <= 123L")
  }

  test("LessThanOrEqual - UTF8String") {
    val p = le("col", "123")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8String() should be (UTF8String.fromString("123"))
    p.resolved() should be (false)
    p.toString should be ("*col <= '123'")
    p.withOrdinal(1).toString should be ("col[1] <= '123'")
  }

  // == In ==
  test("In - int") {
    val p = in("col", Array(3, 2, 1, 0))
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (IntegerType)
    p.getIntArray() should be (Array(0, 1, 2, 3))
    p.resolved() should be (false)
    p.toString should be ("*col isin [0, 1, 2, 3]")
    p.withOrdinal(1).toString should be ("col[1] isin [0, 1, 2, 3]")
  }

  test("In - long") {
    val p = in("col", Array(3L, 2L, 1L, 0L))
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (LongType)
    p.getLongArray() should be (Array(0L, 1L, 2L, 3L))
    p.resolved() should be (false)
    p.toString should be ("*col isin [0L, 1L, 2L, 3L]")
    p.withOrdinal(1).toString should be ("col[1] isin [0L, 1L, 2L, 3L]")
  }

  test("In - UTF8String") {
    val p = in("col", Array("a", "b", "x", "y", "c", "z"))
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (StringType)
    p.getUTF8StringArray() should be (
      Array(UTF8String.fromString("a"), UTF8String.fromString("b"), UTF8String.fromString("c"),
        UTF8String.fromString("x"), UTF8String.fromString("y"), UTF8String.fromString("z")))
    p.resolved() should be (false)
    p.toString should be ("*col isin ['a', 'b', 'c', 'x', 'y', 'z']")
    p.withOrdinal(1).toString should be ("col[1] isin ['a', 'b', 'c', 'x', 'y', 'z']")
  }

  test("IsNull") {
    val p = nvl("col")
    p.name() should be ("col")
    p.ordinal() should be (UNRESOLVED_ORDINAL)
    p.dataType() should be (NullType)
    p.resolved() should be (false)
    p.toString should be ("*col is null")
    p.withOrdinal(1).toString should be ("col[1] is null")
  }

  test("And") {
    var err = intercept[IllegalArgumentException] { and(null, nvl("col")) }
    err.getMessage should be ("Child node is null")
    err = intercept[IllegalArgumentException] { and(nvl("col"), null) }
    err.getMessage should be ("Child node is null")

    val p = and(eqt("col1", 123), gt("col2", 1L))
    p.left should be (eqt("col1", 123))
    p.right should be (gt("col2", 1L))
    p.resolved should be (false)
    p.toString should be ("(*col1 = 123) && (*col2 > 1L)")
  }

  test("Or") {
    var err = intercept[IllegalArgumentException] { or(null, nvl("col")) }
    err.getMessage should be ("Child node is null")
    err = intercept[IllegalArgumentException] { or(nvl("col"), null) }
    err.getMessage should be ("Child node is null")

    val p = or(eqt("col1", 123), gt("col2", 1L))
    p.left should be (eqt("col1", 123))
    p.right should be (gt("col2", 1L))
    p.resolved should be (false)
    p.toString should be ("(*col1 = 123) || (*col2 > 1L)")
  }

  test("Not") {
    val err = intercept[IllegalArgumentException] { FilterApi.not(null) }
    err.getMessage should be ("Child node is null")

    val p = FilterApi.not(eqt("col1", "abc"))
    p.child should be (eqt("col1", "abc"))
    p.resolved should be (false)
    p.toString should be ("!(*col1 = 'abc')")
  }

  test("Trivial") {
    val p1 = TRUE()
    p1.result should be (true)
    val p2 = FALSE()
    p2.result should be (false)
  }
}
