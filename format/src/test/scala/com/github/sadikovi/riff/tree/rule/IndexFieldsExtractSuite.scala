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

package com.github.sadikovi.riff.tree.rule

import org.apache.spark.sql.types._

import com.github.sadikovi.riff.TypeDescription
import com.github.sadikovi.riff.tree.FilterApi
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class IndexFieldsExtractSuite extends UnitTestSuite {
  val schema = StructType(
    StructField("col1", IntegerType) ::
    StructField("col2", StringType) ::
    StructField("col3", LongType) :: Nil)

  test("return trivial tree if no index fields exist") {
    val td = new TypeDescription(schema)
    val tree = and(
      eqt("col1", 1),
      eqt("col2", "abc")
    )
    tree.analyze(td)
    val rule = new IndexFieldsExtract(td)
    val res = tree.transform(rule)
    res should be (and(TRUE, TRUE))
  }

  test("return non-trivial tree if index fields exist") {
    val td = new TypeDescription(schema, Array("col1", "col2"))
    val tree = and(
      eqt("col1", 1),
      or(
        ge("col1", 1),
        le("col2", "abc")
      )
    )
    tree.analyze(td)
    val rule = new IndexFieldsExtract(td)
    val res = tree.transform(rule)
    res should be (tree)
  }

  test("extract fields from AND tree") {
    val td = new TypeDescription(schema, Array("col1"))
    val rule = new IndexFieldsExtract(td)

    // both are non-indexed
    var tree = and(eqt("col2", "abc"), eqt("col2", "abc"))
    var exp = and(TRUE, TRUE)
    tree.analyze(td)
    exp.analyze(td)
    var res = tree.transform(rule)
    res should be (exp)

    // both are indexed
    tree = and(eqt("col1", 1), eqt("col1", 1))
    tree.analyze(td)
    res = tree.transform(rule)
    res should be (tree)

    // one if indexed
    tree = and(eqt("col1", 1), eqt("col2", "abc"))
    exp = and(eqt("col1", 1), TRUE)
    tree.analyze(td)
    exp.analyze(td)
    res = tree.transform(rule)
    res should be (exp)
  }

  test("extract fields from OR tree") {
    val td = new TypeDescription(schema, Array("col1"))
    val rule = new IndexFieldsExtract(td)

    // both are non-indexed
    var tree = or(eqt("col2", "abc"), eqt("col2", "abc"))
    var exp = or(TRUE, TRUE)
    tree.analyze(td)
    exp.analyze(td)
    var res = tree.transform(rule)
    res should be (exp)

    // both are indexed
    tree = or(eqt("col1", 1), eqt("col1", 1))
    tree.analyze(td)
    res = tree.transform(rule)
    res should be (tree)

    // one if indexed
    tree = or(eqt("col1", 1), eqt("col2", "abc"))
    exp = or(eqt("col1", 1), TRUE)
    tree.analyze(td)
    exp.analyze(td)
    res = tree.transform(rule)
    res should be (exp)
  }

  test("extract fields from NOT tree") {
    val td = new TypeDescription(schema, Array("col1"))
    val rule = new IndexFieldsExtract(td)

    var tree = FilterApi.not(eqt("col1", 1))
    tree.analyze(td)
    var res = tree.transform(rule)
    res should be (tree)

    tree = FilterApi.not(eqt("col2", "abc"))
    tree.analyze(td)
    res = tree.transform(rule)
    res should be (TRUE)

    tree = FilterApi.not(nvl("col2"))
    tree.analyze(td)
    res = tree.transform(rule)
    res should be (TRUE)
  }
}
