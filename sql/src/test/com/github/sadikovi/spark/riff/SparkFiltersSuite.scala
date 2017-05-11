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
    Filters.createRiffFilter(Some(And(IsNotNull("col1"), EqualTo("col1", 1)))) should be (
      and(FilterApi.not(nvl("col1")), eqt("col1", 1)))
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
}
