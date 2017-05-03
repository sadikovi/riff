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

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.TypeDescription
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

  test("IsNull predicate") {
    val pre = nvl("col")
    pre.equals(pre) should be (true)
    pre.equals(nvl("col")) should be (true)
    pre.equals(nvl("abc")) should be (false)
    pre.analyzed() should be (false)
    pre.copy() should be (nvl("col"))
    pre.toString should be ("*col is null")

    pre.evaluateState(InternalRow(1), 0) should be (false)
    pre.evaluateState(InternalRow(null), 0) should be (true)

    pre.evaluateState(stats("a", "z", false)) should be (false)
    pre.evaluateState(stats("a", "z", true)) should be (true)

    pre.evaluateState(filter("a")) should be (true)
    pre.evaluateState(filter(1)) should be (true)
    pre.evaluateState(filter(1L)) should be (true)
  }

  test("Not predicate") {
    // using filter api because of the conflicts with scalatest
    val pre = FilterApi.not(eqt("col", 1))
    pre.equals(pre) should be (true)
    pre.equals(FilterApi.not(eqt("col", 1))) should be (true)
    pre.equals(FilterApi.not(gt("col", 1))) should be (false)
    pre.equals(FilterApi.not(eqt("col", 2))) should be (false)
    pre.equals(eqt("col", 1)) should be (false)
    pre.analyzed() should be (false)
    FilterApi.not(FALSE).analyzed() should be (true)
    FilterApi.not(TRUE).analyzed() should be (true)
    pre.copy() should be (pre)
    pre.toString should be ("!(*col = 1)")


    FilterApi.not(TRUE).evaluateState(InternalRow(1)) should be (false)
    FilterApi.not(FALSE).evaluateState(InternalRow(2)) should be (true)

    // we do not apply statistics to the "not" predicate
    FilterApi.not(TRUE).evaluateState(Array(stats("a", "b", false))) should be (true)
    FilterApi.not(TRUE).evaluateState(Array(stats("a", "b", true))) should be (true)
    FilterApi.not(TRUE).evaluateState(Array(stats(1, 4, false))) should be (true)
    FilterApi.not(FALSE).evaluateState(Array(stats(1, 4, true))) should be (true)
    FilterApi.not(FALSE).evaluateState(Array(stats(1L, 4L, false))) should be (true)
    FilterApi.not(FALSE).evaluateState(Array(stats(1L, 4L, true))) should be (true)

    // we do not apply filters to the not predicate
    FilterApi.not(TRUE).evaluateState(Array(filter("a"))) should be (true)
    FilterApi.not(TRUE).evaluateState(Array(filter(2))) should be (true)
    FilterApi.not(FALSE).evaluateState(Array(filter(2))) should be (true)
    FilterApi.not(FALSE).evaluateState(Array(filter(2L))) should be (true)
  }

  test("And predicate") {
    var pre = and(eqt("a", 2), gt("b", 8))
    pre.equals(pre) should be (true)
    pre.equals(and(eqt("a", 2), gt("b", 8))) should be (true)
    pre.equals(and(TRUE, FALSE)) should be (false)
    pre.equals(and(eqt("a", 2), TRUE)) should be (false)
    pre.analyzed() should be (false)
    and(FALSE, FALSE).analyzed() should be (true)
    and(TRUE, FALSE).analyzed() should be (true)
    and(FALSE, TRUE).analyzed() should be (true)
    and(TRUE, TRUE).analyzed() should be (true)
    pre.copy() should be (pre)
    pre.toString should be ("(*a = 2) && (*b > 8)")

    // we need to deal with resolved children here
    and(TRUE, TRUE).evaluateState(InternalRow()) should be (true)
    and(TRUE, FALSE).evaluateState(InternalRow()) should be (false)
    and(FALSE, TRUE).evaluateState(InternalRow()) should be (false)
    and(FALSE, FALSE).evaluateState(InternalRow()) should be (false)

    and(TRUE, TRUE).evaluateState(Array(stats(1, 2, false))) should be (true)
    and(TRUE, FALSE).evaluateState(Array(stats(1, 2, false))) should be (false)
    and(FALSE, TRUE).evaluateState(Array(stats(1, 2, false))) should be (false)
    and(FALSE, FALSE).evaluateState(Array(stats(1, 2, false))) should be (false)

    and(TRUE, TRUE).evaluateState(Array(filter(1))) should be (true)
    and(TRUE, FALSE).evaluateState(Array(filter(1))) should be (false)
    and(FALSE, TRUE).evaluateState(Array(filter(1))) should be (false)
    and(FALSE, FALSE).evaluateState(Array(filter(1))) should be (false)
  }

  test("Or predicate") {
    var pre = or(eqt("a", 2), gt("b", 8))
    pre.equals(pre) should be (true)
    pre.equals(or(eqt("a", 2), gt("b", 8))) should be (true)
    pre.equals(or(TRUE, FALSE)) should be (false)
    pre.equals(or(eqt("a", 2), TRUE)) should be (false)
    pre.analyzed() should be (false)
    or(FALSE, FALSE).analyzed() should be (true)
    or(TRUE, FALSE).analyzed() should be (true)
    or(FALSE, TRUE).analyzed() should be (true)
    or(TRUE, TRUE).analyzed() should be (true)
    pre.copy() should be (pre)
    pre.toString should be ("(*a = 2) || (*b > 8)")

    // we need to deal with resolved children here
    or(TRUE, TRUE).evaluateState(InternalRow()) should be (true)
    or(TRUE, FALSE).evaluateState(InternalRow()) should be (true)
    or(FALSE, TRUE).evaluateState(InternalRow()) should be (true)
    or(FALSE, FALSE).evaluateState(InternalRow()) should be (false)

    or(TRUE, TRUE).evaluateState(Array(stats(1, 2, false))) should be (true)
    or(TRUE, FALSE).evaluateState(Array(stats(1, 2, false))) should be (true)
    or(FALSE, TRUE).evaluateState(Array(stats(1, 2, false))) should be (true)
    or(FALSE, FALSE).evaluateState(Array(stats(1, 2, false))) should be (false)

    or(TRUE, TRUE).evaluateState(Array(filter(1))) should be (true)
    or(TRUE, FALSE).evaluateState(Array(filter(1))) should be (true)
    or(FALSE, TRUE).evaluateState(Array(filter(1))) should be (true)
    or(FALSE, FALSE).evaluateState(Array(filter(1))) should be (false)
  }

  test("FilterApi - analyze full tree") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", IntegerType) ::
      StructField("c", IntegerType) ::
      StructField("d", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = or(
      and(
        eqt("a", 1),
        eqt("b", 8)
      ),
      or(
        FilterApi.not(
          gt("c", 7)
        ),
        le("d", 3)
      )
    )

    tree.analyzed should be (false)
    tree.analyze(td)
    tree.analyzed should be (true)
    tree.toString should be ("((a[0] = 1) && (b[1] = 8)) || ((!(c[2] > 7)) || (d[3] <= 3))")
    // second analysis is no-op and should not change tree
    tree.analyze(td)
    tree.analyzed should be (true)
    tree.toString should be ("((a[0] = 1) && (b[1] = 8)) || ((!(c[2] > 7)) || (d[3] <= 3))")
  }

  test("FilterApi - fail to analyze tree when no such column exist") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", IntegerType) :: Nil)
    val td = new TypeDescription(schema)

    val tree = or(
      and(
        eqt("a", 1),
        le("b", 8L)
      ),
      and(
        gt("c", 3),
        gt("d", 5)
      )
    )
    tree.analyzed should be (false)
    val err = intercept[NoSuchElementException] { tree.analyze(td) }
    err.getMessage should be ("No such field d")
  }

  test("FilterApi - fail to analyze tree when there is a type mismatch") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", StringType) ::
      StructField("c", IntegerType) :: Nil)
    val td = new TypeDescription(schema)

    val tree = or(
      and(
        eqt("a", 1),
        le("b", 8)
      ),
      gt("c", 3)
    )
    tree.analyzed should be (false)
    val err = intercept[IllegalStateException] {
      tree.analyze(td)
    }
    err.getMessage should be ("Type mismatch: StringType != IntegerType, " +
      "spec=TypeSpec(b: string, indexed=false, position=1, origPos=1), tree={b[1] <= 8}")
  }

  test("FilterApi - analyze tree with In predicate") {
    val schema = StructType(
      StructField("a", StringType) ::
      StructField("b", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = or(
      in("a", "v1", "v2", "v3", "v4"),
      eqt("b", 12)
    )

    tree.analyzed should be (false)
    tree.analyze(td)
    tree.analyzed should be (true)
    tree.toString should be ("(a[0] in ['v1', 'v2', 'v3', 'v4']) || (b[1] = 12)")
  }

  test("FilterApi - fail to analyze tree with In predicate type mismatch") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = or(
      in("a", "v1", "v2", "v3", "v4"),
      eqt("b", 12)
    )

    tree.analyzed should be (false)
    val err = intercept[IllegalStateException] {
      tree.analyze(td)
    }
    err.getMessage should be ("Type mismatch: IntegerType != StringType, spec=TypeSpec(a: int, " +
      "indexed=false, position=0, origPos=0), tree={a[0] in ['v1', 'v2', 'v3', 'v4']}")
  }

  test("FilterApi - analyze tree with trivial predicates") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = or(
      TRUE,
      and(
        TRUE,
        FALSE
      )
    )

    tree.analyzed should be (true)
    tree.analyze(td)
    tree.toString should be ("(true) || ((true) && (false))")
  }

  test("FilterApi - analyze tree with non-trivial and trivial predicates") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = and(
      eqt("a", 1),
      or(
        gt("b", 2L),
        FALSE
      )
    )

    tree.analyzed should be (false)
    tree.toString should be ("(*a = 1) && ((*b > 2L) || (false))")
    tree.analyze(td)
    tree.analyzed should be (true)
    tree.toString should be ("(a[0] = 1) && ((b[1] > 2L) || (false))")
  }

  test("FilterApi - evaluate tree for row, stats, and filters") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = and(
      eqt("a", 1),
      or(
        gt("b", 2L),
        lt("a", 3)
      )
    )

    tree.analyze(td)
    tree.analyzed should be (true)

    tree.evaluateState(InternalRow(1, 3L)) should be (true)
    tree.evaluateState(InternalRow(5, 9L)) should be (false)
    tree.evaluateState(InternalRow(3, 9L)) should be (false)
    tree.evaluateState(InternalRow(1, 1L)) should be (true)

    tree.evaluateState(Array(stats(4, 5, false), stats(1L, 1L, true))) should be (false)
    tree.evaluateState(Array(stats(1, 2, false), stats(1L, 1L, true))) should be (true)

    tree.evaluateState(Array(filter(1), filter(3L))) should be (true)
    tree.evaluateState(Array(filter(1), filter(4L))) should be (true)
    tree.evaluateState(Array(filter(2), filter(4L))) should be (false)
  }

  test("FilterApi - evaluate tree with In predicate for row, stats, filters") {
    val schema = StructType(
      StructField("a", StringType) ::
      StructField("b", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = and(
      in("a", "v1", "v2", "v3"),
      eqt("b", 1)
    )

    tree.analyze(td)
    tree.analyzed should be (true)

    tree.evaluateState(InternalRow(UTF8String.fromString("v1"), 1)) should be (true)
    tree.evaluateState(InternalRow(UTF8String.fromString("v2"), 1)) should be (true)
    tree.evaluateState(InternalRow(UTF8String.fromString("v3"), 1)) should be (true)
    tree.evaluateState(InternalRow(UTF8String.fromString("v1"), 2)) should be (false)
    tree.evaluateState(InternalRow(UTF8String.fromString("v2"), 5)) should be (false)
    tree.evaluateState(InternalRow(UTF8String.fromString("va"), 1)) should be (false)

    tree.evaluateState(Array(stats("a", "z", false), stats(1, 2, true))) should be (true)
    tree.evaluateState(Array(stats("v1", "v1", false), stats(1, 1, true))) should be (true)
    tree.evaluateState(Array(stats("x", "z", false), stats(1, 2, true))) should be (false)
    tree.evaluateState(Array(stats("a", "z", false), stats(2, 3, true))) should be (false)

    tree.evaluateState(Array(filter("v1"), filter(1))) should be (true)
    tree.evaluateState(Array(filter("v2"), filter(1))) should be (true)
    tree.evaluateState(Array(filter("v3"), filter(1))) should be (true)
    tree.evaluateState(Array(filter("aa"), filter(1))) should be (false)
    tree.evaluateState(Array(filter("v1"), filter(2))) should be (false)
  }
}
