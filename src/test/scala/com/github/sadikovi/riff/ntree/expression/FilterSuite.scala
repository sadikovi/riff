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

package com.github.sadikovi.riff.ntree

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.RiffTestUtils._
import com.github.sadikovi.riff.ntree.expression._
import com.github.sadikovi.testutil.UnitTestSuite

class FilterSuite extends UnitTestSuite {
  import FilterApi._

  test("FilterApi - objToExpression") {
    var expr = FilterApi.objToExpression(1)
    expr.isInstanceOf[IntegerExpression] should be (true)
    expr.prettyString should be ("1")

    expr = FilterApi.objToExpression(2L)
    expr.isInstanceOf[LongExpression] should be (true)
    expr.prettyString should be ("2L")

    expr = FilterApi.objToExpression("3")
    expr.isInstanceOf[UTF8StringExpression] should be (true)
    expr.prettyString should be ("'3'")
  }

  test("FilterApi - objToExpression, exceptions") {
    val err1 = intercept[NullPointerException] { FilterApi.objToExpression(null) }
    err1.getMessage should be ("Cannot convert null into typed expression")
    val err2 = intercept[UnsupportedOperationException] { FilterApi.objToExpression(true) }
    err2.getMessage should be ("Object true of class java.lang.Boolean")
  }

  test("Trivial predicate") {
    TRUE should be (new Trivial(true))
    FALSE should be (new Trivial(false))

    TRUE.evaluateState(InternalRow()) should be (true)
    TRUE.evaluateState(Array(stats(1, 2, false))) should be (true)
    TRUE.evaluateState(Array(filter(123))) should be (true)
    TRUE.copy() should be (TRUE)
    TRUE.analyzed() should be (true)

    FALSE.evaluateState(InternalRow()) should be (false)
    FALSE.evaluateState(Array(stats(1, 2, false))) should be (false)
    FALSE.evaluateState(Array(filter(123))) should be (false)
    FALSE.copy() should be (FALSE)
    FALSE.analyzed() should be (true)
  }

  test("EqualTo predicate") {
    val pre = eqt("col", 512)
    pre.equals(eqt("col", 513)) should be (false)
    pre.equals(eqt("abc", 512)) should be (false)
    pre.equals(eqt("col", 512)) should be (true)
    pre.analyzed() should be (false)
    pre.copy() should be (eqt("col", 512))
    pre.toString should be ("*col = 512")

    pre.evaluateState(InternalRow(512), 0) should be (true)
    pre.evaluateState(InternalRow(513), 0) should be (false)

    pre.evaluateState(stats(100, 900, false)) should be (true)
    pre.evaluateState(stats(600, 900, false)) should be (false)
    pre.evaluateState(stats(100, 500, false)) should be (false)

    pre.evaluateState(filter(100)) should be (false)
    pre.evaluateState(filter(512)) should be (true)
  }

  test("GreaterThan predicate") {
    val pre = gt("col", 500)
    pre.equals(gt("col", 500)) should be (true)
    pre.equals(gt("abc", 500)) should be (false)
    pre.equals(gt("col", -10)) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (gt("col", 500))
    pre.toString should be ("*col > 500")

    pre.evaluateState(InternalRow(501), 0) should be (true)
    pre.evaluateState(InternalRow(500), 0) should be (false)

    pre.evaluateState(stats(100, 900, false)) should be (true)
    pre.evaluateState(stats(600, 900, false)) should be (true)
    pre.evaluateState(stats(100, 500, false)) should be (false)

    pre.evaluateState(filter(100)) should be (true)
    pre.evaluateState(filter(500)) should be (true)
  }

  test("LessThan predicate") {
    val pre = lt("col", 500)
    pre.equals(lt("col", 500)) should be (true)
    pre.equals(lt("abc", 500)) should be (false)
    pre.equals(lt("col", -10)) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (lt("col", 500))
    pre.toString should be ("*col < 500")

    pre.evaluateState(InternalRow(499), 0) should be (true)
    pre.evaluateState(InternalRow(500), 0) should be (false)

    pre.evaluateState(stats(100, 900, false)) should be (true)
    pre.evaluateState(stats(500, 900, false)) should be (false)
    pre.evaluateState(stats(100, 500, false)) should be (true)

    pre.evaluateState(filter(100)) should be (true)
    pre.evaluateState(filter(500)) should be (true)
  }

  test("GreaterThanOrEqual predicate") {
    val pre = ge("col", 500)
    pre.equals(ge("col", 500)) should be (true)
    pre.equals(ge("abc", 500)) should be (false)
    pre.equals(ge("col", -10)) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (ge("col", 500))
    pre.toString should be ("*col >= 500")

    pre.evaluateState(InternalRow(499), 0) should be (false)
    pre.evaluateState(InternalRow(500), 0) should be (true)
    pre.evaluateState(InternalRow(501), 0) should be (true)

    pre.evaluateState(stats(100, 900, false)) should be (true)
    pre.evaluateState(stats(500, 900, false)) should be (true)
    pre.evaluateState(stats(100, 500, false)) should be (true)
    pre.evaluateState(stats(100, 499, false)) should be (false)

    pre.evaluateState(filter(100)) should be (true)
    pre.evaluateState(filter(500)) should be (true)
    pre.evaluateState(filter(900)) should be (true)
  }

  test("LessThanOrEqual predicate") {
    val pre = le("col", 500)
    pre.equals(le("col", 500)) should be (true)
    pre.equals(le("abc", 500)) should be (false)
    pre.equals(le("col", -10)) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (le("col", 500))
    pre.toString should be ("*col <= 500")

    pre.evaluateState(InternalRow(499), 0) should be (true)
    pre.evaluateState(InternalRow(500), 0) should be (true)
    pre.evaluateState(InternalRow(501), 0) should be (false)

    pre.evaluateState(stats(100, 900, false)) should be (true)
    pre.evaluateState(stats(500, 900, false)) should be (true)
    pre.evaluateState(stats(501, 900, false)) should be (false)
    pre.evaluateState(stats(100, 500, false)) should be (true)

    pre.evaluateState(filter(100)) should be (true)
    pre.evaluateState(filter(500)) should be (true)
    pre.evaluateState(filter(900)) should be (true)
  }

  test("In predicate - validation") {
    val err1 = intercept[IllegalArgumentException] { in("col") }
    err1.getMessage should be ("Empty list of expressions for In predicate")
    val err2 = intercept[IllegalArgumentException] { in("col", "a",
      1.asInstanceOf[java.lang.Integer], "b") }
    err2.getMessage should be ("Invalid data type, expected StringType, found IntegerType")
  }

  test("In predicate") {
    val pre = in("col", "a", "k", "c", "f", "x", "n", "n")
    pre.equals(in("col", "a", "k", "c", "f", "x", "n"), true)
    pre.equals(in("col", "a", "k", "c", "f")) should be (false)
    pre.equals(in("abc", "a", "k", "c", "f", "x", "n")) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (in("col", "a", "k", "c", "f", "x", "n"))
    pre.toString should be ("*col in ['a', 'c', 'f', 'k', 'n', 'x']")
    pre.copy().toString should be ("*col in ['a', 'c', 'f', 'k', 'n', 'x']")

    pre.evaluateState(InternalRow(UTF8String.fromString("a")), 0) should be (true)
    pre.evaluateState(InternalRow(UTF8String.fromString("k")), 0) should be (true)
    pre.evaluateState(InternalRow(UTF8String.fromString("b")), 0) should be (false)
    pre.evaluateState(InternalRow(UTF8String.fromString("d")), 0) should be (false)
    pre.evaluateState(InternalRow(UTF8String.fromString("z")), 0) should be (false)

    pre.evaluateState(stats("a", "z", false)) should be (true)
    pre.evaluateState(stats("a", "b", false)) should be (true)
    pre.evaluateState(stats("f", "f", false)) should be (true)
    pre.evaluateState(stats("y", "z", false)) should be (false)
    pre.evaluateState(stats("l", "m", false)) should be (false)

    pre.evaluateState(filter("a")) should be (true)
    pre.evaluateState(filter("f")) should be (true)
    pre.evaluateState(filter("y")) should be (false)
    pre.evaluateState(filter("z")) should be (false)
  }
}
