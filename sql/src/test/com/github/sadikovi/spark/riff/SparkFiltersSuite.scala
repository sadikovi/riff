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

package com.github.sadikovi.spark.riff

import org.apache.spark.sql.sources._

import com.github.sadikovi.riff.tree.FilterApi
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class SparkFiltersSuite extends UnitTestSuite {
  test("check references") {
    assert(Filters.references(IsNull("col1")) == Filters.references(IsNull("col1")))
    assert(Filters.references(IsNull("col1")) == Filters.references(EqualTo("col1", 1)))
    assert(Filters.references(
      Or(
        EqualTo("col1", 1),
        GreaterThan("col2", 2)
      )
    ) == Filters.references(
      And(
        EqualTo("col1", 3),
        LessThan("col2", 0)
      )
    ))

    // check that order of tree nodes is the same
    assert(Filters.references(
      And(
        EqualTo("col1", 1),
        EqualTo("col2", 1)
      )
    ) != Filters.references(
      And(
        EqualTo("col2", 1),
        EqualTo("col1", 1)
      )
    ))
  }

  test("isLeaf") {
    Filters.isLeaf(EqualTo("col", 1)) should be (true)
    Filters.isLeaf(GreaterThan("col", 1)) should be (true)
    Filters.isLeaf(LessThan("col", 1)) should be (true)
    Filters.isLeaf(IsNull("col")) should be (true)
    Filters.isLeaf(IsNotNull("col")) should be (true)

    Filters.isLeaf(And(EqualTo("col", 1), EqualTo("col", 1))) should be (false)
    Filters.isLeaf(Or(EqualTo("col", 1), EqualTo("col", 1))) should be (false)
    Filters.isLeaf(Not(EqualTo("col", 1))) should be (false)
  }

  test("isNullRelated") {
    Filters.isNullRelated(EqualTo("col", 1)) should be (false)
    Filters.isNullRelated(GreaterThan("col", 1)) should be (false)
    Filters.isNullRelated(And(EqualTo("col", 1), EqualTo("col", 1))) should be (false)

    Filters.isNullRelated(IsNull("col")) should be (true)
    Filters.isNullRelated(IsNotNull("col")) should be (true)
  }

  test("convert None filter into tree") {
    Filters.createRiffFilter(None) should be (null)
  }

  test("convert EqualTo into tree") {
    Filters.createRiffFilter(Some(EqualTo("col", "abc"))) should be (eqt("col", "abc"))
    Filters.createRiffFilter(Some(EqualTo("col", 1))) should be (eqt("col", 1))
    Filters.createRiffFilter(Some(EqualTo("col", 1L))) should be (eqt("col", 1L))
    Filters.createRiffFilter(Some(EqualTo("col", null))) should be (nvl("col"))
  }

  test("convert EqualNullSafe into tree") {
    Filters.createRiffFilter(Some(EqualNullSafe("col", "abc"))) should be (eqt("col", "abc"))
    Filters.createRiffFilter(Some(EqualNullSafe("col", 1))) should be (eqt("col", 1))
    Filters.createRiffFilter(Some(EqualNullSafe("col", 1L))) should be (eqt("col", 1L))
    Filters.createRiffFilter(Some(EqualNullSafe("col", null))) should be (nvl("col"))
  }

  test("convert GreaterThan into tree") {
    Filters.createRiffFilter(Some(GreaterThan("col", "abc"))) should be (gt("col", "abc"))
    Filters.createRiffFilter(Some(GreaterThan("col", 1))) should be (gt("col", 1))
  }

  test("convert GreaterThanOrEqual into tree") {
    Filters.createRiffFilter(Some(GreaterThanOrEqual("col", "abc"))) should be (ge("col", "abc"))
    Filters.createRiffFilter(Some(GreaterThanOrEqual("col", 1))) should be (ge("col", 1))
  }

  test("convert LessThan into tree") {
    Filters.createRiffFilter(Some(LessThan("col", "abc"))) should be (lt("col", "abc"))
    Filters.createRiffFilter(Some(LessThan("col", 1))) should be (lt("col", 1))
  }

  test("convert LessThanOrEqual into tree") {
    Filters.createRiffFilter(Some(LessThanOrEqual("col", "abc"))) should be (le("col", "abc"))
    Filters.createRiffFilter(Some(LessThanOrEqual("col", 1))) should be (le("col", 1))
  }

  test("convert In into tree") {
    Filters.createRiffFilter(Some(In("col", Array(1, 2)))) should be (
      in("col", 1.asInstanceOf[java.lang.Integer], 2.asInstanceOf[java.lang.Integer]))
    Filters.createRiffFilter(Some(In("col", Array("a", "b")))) should be (in("col", "a", "b"))
  }

  test("convert IsNull into tree") {
    Filters.createRiffFilter(Some(IsNull("col"))) should be (nvl("col"))
  }

  test("convert IsNotNull into tree") {
    // we do not have IsNotNull tree node, so we convert it into negation of IsNull
    Filters.createRiffFilter(Some(IsNotNull("col"))) should be (FilterApi.not(nvl("col")))
  }

  test("convert And into tree") {
    Filters.createRiffFilter(Some(And(EqualTo("col1", "a"), GreaterThan("col2", 1)))) should be (
      and(eqt("col1", "a"), gt("col2", 1)))
    Filters.createRiffFilter(Some(And(IsNotNull("col2"), EqualTo("col1", 1)))) should be (
      and(FilterApi.not(nvl("col2")), eqt("col1", 1)))
  }

  test("convert Or into tree") {
    Filters.createRiffFilter(Some(Or(LessThan("col1", 1L), IsNull("col2")))) should be (
      or(lt("col1", 1L), nvl("col2")))
  }

  test("convert Not into tree") {
    Filters.createRiffFilter(Some(Not(EqualTo("col", 2)))) should be (
      FilterApi.not(eqt("col", 2)))
  }

  test("unsupported filters") {
    Filters.createRiffFilter(Some(StringStartsWith("col", "abc"))) should be (TRUE)
    Filters.createRiffFilter(Some(StringEndsWith("col", "abc"))) should be (TRUE)
    Filters.createRiffFilter(Some(StringContains("col", "abc"))) should be (TRUE)
  }

  test("remove IsNotNull in conjunction with leaf tree nodes") {
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), EqualTo("col1", 1)))) should be (
      eqt("col1", 1))
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), GreaterThan("col1", 1)))) should be (
      gt("col1", 1))
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), LessThan("col1", 1)))) should be (
      lt("col1", 1))
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), In("col1", Array("a", "b"))))) should be (
      in("col1", "a", "b"))

    // remove when filters are swapped
    Filters.createRiffFilter(Some(And(EqualTo("col1", 1), IsNotNull("col1")))) should be (
      eqt("col1", 1))
  }

  test("do not remove IsNotNull when conditions do not hold") {
    // do not remove conjunction when references do not mach
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), EqualTo("col2", 1)))) should be (
      and(FilterApi.not(nvl("col1")), eqt("col2", 1)))
    // do not remove conjunction when both filters are null related
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), IsNull("col1")))) should be (
      and(FilterApi.not(nvl("col1")), nvl("col1")))
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), IsNotNull("col1")))) should be (
      and(FilterApi.not(nvl("col1")), FilterApi.not(nvl("col1"))))
    // do not remove conjunction when children are non-leaf
    Filters.createRiffFilter(Some(
      And(
        IsNotNull("col1"),
        And(
          EqualTo("col1", 1),
          EqualTo("col1", 1)
        )
      )
    )) should be (and(FilterApi.not(nvl("col1")), and(eqt("col1", 1), eqt("col1", 1))))
  }
}
