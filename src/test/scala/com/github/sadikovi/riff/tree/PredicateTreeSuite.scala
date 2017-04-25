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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.Statistics
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class PredicateTreeSuite extends UnitTestSuite {
  // == EqualTo ==
  test("EqualTo (int) - evaluate row") {
    val row = InternalRow(10)
    eqt("col", 1).withOrdinal(0).evaluate(row) should be (false)
    eqt("col", 10).withOrdinal(0).evaluate(row) should be (true)
    eqt("col", 11).withOrdinal(0).evaluate(row) should be (false)
  }

  test("EqualTo (long) - evaluate row") {
    val row = InternalRow(10L)
    eqt("col", 1L).withOrdinal(0).evaluate(row) should be (false)
    eqt("col", 10L).withOrdinal(0).evaluate(row) should be (true)
    eqt("col", 11L).withOrdinal(0).evaluate(row) should be (false)
  }

  test("EqualTo (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("iii"))
    eqt("col", "aaa").withOrdinal(0).evaluate(row) should be (false)
    eqt("col", "iii").withOrdinal(0).evaluate(row) should be (true)
    eqt("col", "zzz").withOrdinal(0).evaluate(row) should be (false)
  }

  // == GreaterThan ==
  test("GreaterThan (int) - evaluate row") {
    val row = InternalRow(10)
    gt("col", 1).withOrdinal(0).evaluate(row) should be (true)
    gt("col", 100).withOrdinal(0).evaluate(row) should be (false)
  }

  test("GreaterThan (long) - evaluate row") {
    val row = InternalRow(10L)
    gt("col", 1L).withOrdinal(0).evaluate(row) should be (true)
    gt("col", 100L).withOrdinal(0).evaluate(row) should be (false)
  }

  test("GreaterThan (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("iii"))
    gt("col", "aaa").withOrdinal(0).evaluate(row) should be (true)
    gt("col", "zzz").withOrdinal(0).evaluate(row) should be (false)
  }

  // == LessThan ==
  test("LessThan (int) - evaluate row") {
    val row = InternalRow(10)
    lt("col", 1).withOrdinal(0).evaluate(row) should be (false)
    lt("col", 100).withOrdinal(0).evaluate(row) should be (true)
  }

  test("LessThan (long) - evaluate row") {
    val row = InternalRow(10L)
    lt("col", 1L).withOrdinal(0).evaluate(row) should be (false)
    lt("col", 100L).withOrdinal(0).evaluate(row) should be (true)
  }

  test("LessThan (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("iii"))
    lt("col", "aaa").withOrdinal(0).evaluate(row) should be (false)
    lt("col", "zzz").withOrdinal(0).evaluate(row) should be (true)
  }

  // == GreaterThanOrEqual ==
  test("GreaterThanOrEqual (int) - evaluate row") {
    val row = InternalRow(10)
    ge("col", 1).withOrdinal(0).evaluate(row) should be (true)
    ge("col", 10).withOrdinal(0).evaluate(row) should be (true)
    ge("col", 100).withOrdinal(0).evaluate(row) should be (false)
  }

  test("GreaterThanOrEqual (long) - evaluate row") {
    val row = InternalRow(10L)
    ge("col", 1L).withOrdinal(0).evaluate(row) should be (true)
    ge("col", 10L).withOrdinal(0).evaluate(row) should be (true)
    ge("col", 100L).withOrdinal(0).evaluate(row) should be (false)
  }

  test("GreaterThanOrEqual (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("iii"))
    ge("col", "aaa").withOrdinal(0).evaluate(row) should be (true)
    ge("col", "iii").withOrdinal(0).evaluate(row) should be (true)
    ge("col", "zzz").withOrdinal(0).evaluate(row) should be (false)
  }

  // == LessThanOrEqual ==
  test("LessThanOrEqual (int) - evaluate row") {
    val row = InternalRow(10)
    le("col", 1).withOrdinal(0).evaluate(row) should be (false)
    le("col", 10).withOrdinal(0).evaluate(row) should be (true)
    le("col", 100).withOrdinal(0).evaluate(row) should be (true)
  }

  test("LessThanOrEqual (long) - evaluate row") {
    val row = InternalRow(10L)
    le("col", 1L).withOrdinal(0).evaluate(row) should be (false)
    le("col", 10L).withOrdinal(0).evaluate(row) should be (true)
    le("col", 100L).withOrdinal(0).evaluate(row) should be (true)
  }

  test("LessThanOrEqual (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("iii"))
    le("col", "aaa").withOrdinal(0).evaluate(row) should be (false)
    le("col", "iii").withOrdinal(0).evaluate(row) should be (true)
    le("col", "zzz").withOrdinal(0).evaluate(row) should be (true)
  }

  // == In ==
  test("In (int) - evaluate row") {
    val row = InternalRow(10)
    in("col", Array(1, 2, 3)).withOrdinal(0).evaluate(row) should be (false)
    in("col", Array(10, 2, 3)).withOrdinal(0).evaluate(row) should be (true)
    in("col", Array(10, 10, 10)).withOrdinal(0).evaluate(row) should be (true)
  }

  test("In (long) - evaluate row") {
    val row = InternalRow(10L)
    in("col", Array(1L, 2L, 3L)).withOrdinal(0).evaluate(row) should be (false)
    in("col", Array(10L, 2L, 3L)).withOrdinal(0).evaluate(row) should be (true)
    in("col", Array(10L, 10L, 10L)).withOrdinal(0).evaluate(row) should be (true)
  }

  test("In (UTF8) - evaluate row") {
    val row = InternalRow(UTF8String.fromString("i"))
    in("col", Array("a", "b", "c")).withOrdinal(0).evaluate(row) should be (false)
    in("col", Array("i", "b", "x")).withOrdinal(0).evaluate(row) should be (true)
    in("col", Array("i", "i", "i")).withOrdinal(0).evaluate(row) should be (true)
  }

  // == null tests ==
  test("EqualTo - null value") {
    eqt("col", 1).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    eqt("col", 1L).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    eqt("col", "1").withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("GreaterThan - null value") {
    gt("col", 1).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    gt("col", 1L).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    gt("col", "1").withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("LessThan - null value") {
    lt("col", 1).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    lt("col", 1L).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    lt("col", "1").withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("GreaterThanOrEqual - null value") {
    ge("col", 1).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    ge("col", 1L).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    ge("col", "1").withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("LessThanOrEqual - null value") {
    le("col", 1).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    le("col", 1L).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    le("col", "1").withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("In - null value") {
    in("col", Array(1, 2, 3)).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    in("col", Array(1L, 2L, 3L)).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
    in("col", Array("1", "2", "3")).withOrdinal(0).evaluate(InternalRow(null)) should be (false)
  }

  test("IsNull - null value") {
    nvl("col").withOrdinal(0).evaluate(InternalRow(123)) should be (false)
    nvl("col").withOrdinal(0).evaluate(InternalRow(null)) should be (true)
  }

  // == Statistics tests ==
  test("int filters - statistics") {
    val stats = Statistics.sqlTypeToStatistics(IntegerType)
    stats.update(InternalRow(1), 0)
    stats.update(InternalRow(123), 0)
    stats.update(InternalRow(null), 0)

    eqt("col", 1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", 56).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", 123).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", -1).withOrdinal(0).evaluate(Array(stats)) should be (false)
    eqt("col", 124).withOrdinal(0).evaluate(Array(stats)) should be (false)

    gt("col", 1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 56).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 123).withOrdinal(0).evaluate(Array(stats)) should be (false)
    gt("col", -1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 124).withOrdinal(0).evaluate(Array(stats)) should be (false)

    lt("col", 1).withOrdinal(0).evaluate(Array(stats)) should be (false)
    lt("col", 56).withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", 123).withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", -1).withOrdinal(0).evaluate(Array(stats)) should be (false)
    lt("col", 124).withOrdinal(0).evaluate(Array(stats)) should be (true)

    ge("col", 1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 56).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 123).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", -1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 124).withOrdinal(0).evaluate(Array(stats)) should be (false)

    le("col", 1).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", 56).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", 123).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", -1).withOrdinal(0).evaluate(Array(stats)) should be (false)
    le("col", 124).withOrdinal(0).evaluate(Array(stats)) should be (true)
  }

  test("long filters - statistics") {
    val stats = Statistics.sqlTypeToStatistics(LongType)
    stats.update(InternalRow(1L), 0)
    stats.update(InternalRow(123L), 0)
    stats.update(InternalRow(null), 0)

    eqt("col", 1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", 56L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", 123L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", -1L).withOrdinal(0).evaluate(Array(stats)) should be (false)
    eqt("col", 124L).withOrdinal(0).evaluate(Array(stats)) should be (false)

    gt("col", 1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 56L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 123L).withOrdinal(0).evaluate(Array(stats)) should be (false)
    gt("col", -1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", 124L).withOrdinal(0).evaluate(Array(stats)) should be (false)

    lt("col", 1L).withOrdinal(0).evaluate(Array(stats)) should be (false)
    lt("col", 56L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", 123L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", -1L).withOrdinal(0).evaluate(Array(stats)) should be (false)
    lt("col", 124L).withOrdinal(0).evaluate(Array(stats)) should be (true)

    ge("col", 1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 56L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 123L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", -1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", 124L).withOrdinal(0).evaluate(Array(stats)) should be (false)

    le("col", 1L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", 56L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", 123L).withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", -1L).withOrdinal(0).evaluate(Array(stats)) should be (false)
    le("col", 124L).withOrdinal(0).evaluate(Array(stats)) should be (true)
  }

  test("UTF8 filters - statistics") {
    val stats = Statistics.sqlTypeToStatistics(StringType)
    stats.update(InternalRow(UTF8String.fromString("100")), 0)
    stats.update(InternalRow(UTF8String.fromString("123")), 0)
    stats.update(InternalRow(null), 0)

    eqt("col", "100").withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", "112").withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", "123").withOrdinal(0).evaluate(Array(stats)) should be (true)
    eqt("col", "560").withOrdinal(0).evaluate(Array(stats)) should be (false)
    eqt("col", "000").withOrdinal(0).evaluate(Array(stats)) should be (false)

    gt("col", "100").withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", "112").withOrdinal(0).evaluate(Array(stats)) should be (true)
    gt("col", "123").withOrdinal(0).evaluate(Array(stats)) should be (false)
    gt("col", "560").withOrdinal(0).evaluate(Array(stats)) should be (false)
    gt("col", "000").withOrdinal(0).evaluate(Array(stats)) should be (true)

    lt("col", "100").withOrdinal(0).evaluate(Array(stats)) should be (false)
    lt("col", "112").withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", "123").withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", "560").withOrdinal(0).evaluate(Array(stats)) should be (true)
    lt("col", "000").withOrdinal(0).evaluate(Array(stats)) should be (false)

    ge("col", "100").withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", "112").withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", "123").withOrdinal(0).evaluate(Array(stats)) should be (true)
    ge("col", "560").withOrdinal(0).evaluate(Array(stats)) should be (false)
    ge("col", "000").withOrdinal(0).evaluate(Array(stats)) should be (true)

    le("col", "100").withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", "112").withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", "123").withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", "560").withOrdinal(0).evaluate(Array(stats)) should be (true)
    le("col", "000").withOrdinal(0).evaluate(Array(stats)) should be (false)
  }

  test("IsNull - statistics positive") {
    val stats = Statistics.sqlTypeToStatistics(IntegerType)
    stats.update(InternalRow(1), 0)
    stats.update(InternalRow(123), 0)
    stats.update(InternalRow(null), 0)
    nvl("col").withOrdinal(0).evaluate(Array(stats)) should be (true)
  }

  test("IsNull - statistics negative") {
    val stats = Statistics.sqlTypeToStatistics(IntegerType)
    stats.update(InternalRow(1), 0)
    stats.update(InternalRow(123), 0)
    nvl("col").withOrdinal(0).evaluate(Array(stats)) should be (false)
  }

  test("In - statistics positive") {
    val stats = Array(
      Statistics.sqlTypeToStatistics(IntegerType),
      Statistics.sqlTypeToStatistics(LongType),
      Statistics.sqlTypeToStatistics(StringType))
    for (i <- 0 until 3) {
      stats(i).update(InternalRow(1, 1L, UTF8String.fromString("1")), i)
      stats(i).update(InternalRow(123, 123L, UTF8String.fromString("3")), i)
      stats(i).update(InternalRow(null, null, null), i)
    }

    in("col", Array(1, 2, 3)).withOrdinal(0).evaluate(stats) should be (true)
    in("col", Array(-1, 1, -3)).withOrdinal(0).evaluate(stats) should be (true)
    in("col", Array(-1, -2, -3)).withOrdinal(0).evaluate(stats) should be (false)

    in("col", Array(1L, 2L, 3L)).withOrdinal(1).evaluate(stats) should be (true)
    in("col", Array(-1L, 1L, -3L)).withOrdinal(1).evaluate(stats) should be (true)
    in("col", Array(-1L, -2L, -3L)).withOrdinal(1).evaluate(stats) should be (false)

    in("col", Array("1", "2", "3")).withOrdinal(2).evaluate(stats) should be (true)
    in("col", Array("0", "1", "5")).withOrdinal(2).evaluate(stats) should be (true)
    in("col", Array("0", "0", "0")).withOrdinal(2).evaluate(stats) should be (false)
  }

  // == Logical nodes tests ==
  test("evalute logical nodes predicate") {
    val p = and(
      eqt("col1", 123).withOrdinal(0),
      or(
        eqt("col2", 123L).withOrdinal(1),
        gt("col3", "aaa").withOrdinal(2)
      )
    )
    p.evaluate(InternalRow(123, 123L, UTF8String.fromString("aaa"))) should be (true)
    p.evaluate(InternalRow(123, null, UTF8String.fromString("bbb"))) should be (true)
    p.evaluate(InternalRow(123, null, UTF8String.fromString("aaa"))) should be (false)
    p.evaluate(InternalRow(321, 123L, UTF8String.fromString("bbb"))) should be (false)
    p.evaluate(InternalRow(null, null, null)) should be (false)
  }

  test("evaluate Not predicate") {
    val p = FilterApi.not(eqt("col", 1).withOrdinal(0))
    p.evaluate(InternalRow(1)) should be (false)
    p.evaluate(InternalRow(2)) should be (true)
  }
}
