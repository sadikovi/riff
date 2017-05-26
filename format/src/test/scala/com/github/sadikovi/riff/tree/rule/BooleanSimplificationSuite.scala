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

import com.github.sadikovi.riff.tree.FilterApi
import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class BooleanSimplificationSuite extends UnitTestSuite {
  test("simplify AND tree") {
    val rule = new BooleanSimplification()
    and(TRUE, FALSE).transform(rule) should be (FALSE)
    and(TRUE, TRUE).transform(rule) should be (TRUE)
    and(FALSE, TRUE).transform(rule) should be (FALSE)
    and(FALSE, FALSE).transform(rule) should be (FALSE)
    and(eqt("col", 1), TRUE).transform(rule) should be (eqt("col", 1))
    and(FALSE, eqt("col", 1)).transform(rule) should be (FALSE)
  }

  test("simplify OR tree") {
    val rule = new BooleanSimplification()
    or(TRUE, FALSE).transform(rule) should be (TRUE)
    or(TRUE, TRUE).transform(rule) should be (TRUE)
    or(FALSE, TRUE).transform(rule) should be (TRUE)
    or(FALSE, FALSE).transform(rule) should be (FALSE)
    or(eqt("col", 1), TRUE).transform(rule) should be (TRUE)
    or(FALSE, eqt("col", 1)).transform(rule) should be (eqt("col", 1))
  }

  test("simplify NOT tree") {
    val rule = new BooleanSimplification()
    FilterApi.not(TRUE).transform(rule) should be (FALSE)
    FilterApi.not(FALSE).transform(rule) should be (TRUE)
    FilterApi.not(gt("col", 1)).transform(rule) should be (FilterApi.not(gt("col", 1)))
  }

  test("simplify predicate containing non-trivial tree") {
    val tree = and(eqt("col", 1), gt("col", 2))
    val rule = new BooleanSimplification()
    val res = tree.transform(rule)
    res should be (tree)
  }

  test("simplify predicate containing mix of trivial and non-trivial tree") {
    val tree = and(
      or(
        eqt("col", 1),
        TRUE
      ),
      and(
        le("col", "abc"),
        TRUE
      )
    )
    val rule = new BooleanSimplification()
    val res = tree.transform(rule)
    res should be (le("col", "abc"))
  }

  test("simplify predicate containing trivial tree") {
    val tree = and(
      or(
        FALSE,
        FilterApi.not(FALSE)
      ),
      and(
        TRUE,
        FALSE
      )
    )
    val rule = new BooleanSimplification()
    val res = tree.transform(rule)
    res should be (FALSE)
  }
}
