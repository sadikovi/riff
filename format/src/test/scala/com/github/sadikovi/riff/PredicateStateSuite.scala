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

import com.github.sadikovi.riff.tree.{FilterApi, State}
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class PredicateStateSuite extends UnitTestSuite {
  test("fail to initialize") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    var err = intercept[IllegalArgumentException] {
      new PredicateState(null, new TypeDescription(schema))
    }
    err.getMessage should be ("Tree is null")

    err = intercept[IllegalArgumentException] {
      new PredicateState(nvl("col"), null)
    }
    err.getMessage should be ("Type description is null")
  }

  test("tree can be already resolved") {
    val schema = StructType(StructField("col", IntegerType) :: Nil)
    val td = new TypeDescription(schema)
    val tree = nvl("col")
    tree.analyze(td)
    val state = new PredicateState(tree, td)
    state.indexTree should be (TRUE)
  }

  test("fail to resolve tree because of non-existent column") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = eqt("col4", "abc")
    val err = intercept[NoSuchElementException] {
      new PredicateState(p, td)
    }
    err.getMessage should be ("No such field col4")
  }

  test("fail to resolve tree because of type mismatch") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = eqt("col1", "abc")
    val err = intercept[IllegalStateException] {
      new PredicateState(p, td)
    }
    assert(err.getMessage.contains("Type mismatch"))
  }

  test("generic predicate and type includes index and data fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = and(eqt("col2", "abc"), and(gt("col3", 47L), eqt("col1", 12)))
    val state = new PredicateState(p.copy(), td)
    p.analyze(td)
    state.tree should be (p)
    val indexTree = eqt("col1", 12)
    indexTree.analyze(td)
    state.indexTree should be (indexTree)
    state.hasIndexedTreeOnly should be (false)
  }

  test("predicate includes index and data fields, index tree is resolved to TRUE") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = and(eqt("col2", "abc"), or(gt("col3", 47L), eqt("col1", 12)))
    val state = new PredicateState(p, td)
    p.analyze(td)
    state.tree should be (p)
    state.indexTree should be (TRUE)
    state.hasIndexedTreeOnly should be (false)
  }

  test("predicate includes only index fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = or(eqt("col1", 12), gt("col1", 47))
    val state = new PredicateState(p, td)
    p.analyze(td)
    state.hasIndexedTreeOnly should be (true)
    state.indexTree should be (p)
    state.tree should be (null)
  }

  test("predicate includes only data fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", LongType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = or(eqt("col3", 12L), gt("col2", "abc"))
    val state = new PredicateState(p, td)
    p.analyze(td)
    state.hasIndexedTreeOnly should be (false)
    state.indexTree should be (TRUE)
    state.tree should be (p)
  }

  test("check result() for predicate state") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = or(eqt("col1", 12), gt("col2", "abc"))
    val state = new PredicateState(p, td)
    state.result should be (State.Unknown)
  }

  test("check result() for index predicate state") {
    val schema = StructType(
      StructField("col1", IntegerType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = eqt("col1", 12)
    val state = new PredicateState(p, td)
    state.result should be (State.Unknown)
  }

  test("check result() for trivial predicate state") {
    val schema = StructType(
      StructField("col1", IntegerType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val state = new PredicateState(or(TRUE, eqt("col1", 1)), td)
    state.result should be (State.True)
  }

  test("check result() for non-trivial predicate state") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    val p = or(eqt("col1", 12), gt("col2", "abc"))
    val state = new PredicateState(p, td)
    state.result should be (State.Unknown)
  }

  test("check result() for predicate state with binary logical") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    new PredicateState(or(TRUE, eqt("col1", 1)), td).result() should be (State.True)
    new PredicateState(and(FALSE, eqt("col1", 1)), td).result() should be (State.False)
  }

  test("predicate state for filter Not(IsNull) that does not contain index fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col2"))
    val p = and(
      FilterApi.not(nvl("col1")),
      eqt("col1", 1)
    )
    val state = new PredicateState(p, td)
    p.analyze(td)
    state.indexTree() should be (TRUE)
    state.tree() should be (p)
  }
}
