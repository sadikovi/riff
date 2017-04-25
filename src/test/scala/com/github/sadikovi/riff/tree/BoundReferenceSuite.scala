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

import com.github.sadikovi.riff.tree.FilterApi._
import com.github.sadikovi.testutil.UnitTestSuite

class BoundReferenceSuite extends UnitTestSuite {
  // == EqualTo ==
  test("EqualTo (int) - bound reference methods") {
    val p = eqt("col", 123)
    p.equals(p) should be (true)
    p.equals(eqt("col", 123)) should be (true)
    p.equals(eqt("abc", 123)) should be (false)
    p.equals(eqt("col", 124)) should be (false)
    p.equals(eqt("col", 123L)) should be (false)
    p.equals(eqt("col", "123")) should be (false)
    p.hashCode should be (eqt("col", 123).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("EqualTo (long) - bound reference methods") {
    val p = eqt("col", 123L)
    p.equals(p) should be (true)
    p.equals(eqt("col", 123L)) should be (true)
    p.equals(eqt("abc", 123L)) should be (false)
    p.equals(eqt("col", 124L)) should be (false)
    p.equals(eqt("col", 123)) should be (false)
    p.equals(eqt("col", "123")) should be (false)
    p.hashCode should be (eqt("col", 123L).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("EqualTo (UTF8) - bound reference methods") {
    val p = eqt("col", "123")
    p.equals(p) should be (true)
    p.equals(eqt("col", "123")) should be (true)
    p.equals(eqt("abc", "123")) should be (false)
    p.equals(eqt("col", "124")) should be (false)
    p.equals(eqt("col", 123)) should be (false)
    p.equals(eqt("col", 123L)) should be (false)
    p.hashCode should be (eqt("col", "123").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == GreaterThan ==
  test("GreaterThan (int) - bound reference methods") {
    val p = gt("col", 123)
    p.equals(p) should be (true)
    p.equals(gt("col", 123)) should be (true)
    p.equals(gt("abc", 123)) should be (false)
    p.equals(gt("col", 124)) should be (false)
    p.equals(gt("col", 123L)) should be (false)
    p.equals(gt("col", "123")) should be (false)
    p.hashCode should be (gt("col", 123).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("GreaterThan (long) - bound reference methods") {
    val p = gt("col", 123L)
    p.equals(p) should be (true)
    p.equals(gt("col", 123L)) should be (true)
    p.equals(gt("abc", 123L)) should be (false)
    p.equals(gt("col", 124L)) should be (false)
    p.equals(gt("col", 123)) should be (false)
    p.equals(gt("col", "123")) should be (false)
    p.hashCode should be (gt("col", 123L).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("GreaterThan (UTF8) - bound reference methods") {
    val p = gt("col", "123")
    p.equals(p) should be (true)
    p.equals(gt("col", "123")) should be (true)
    p.equals(gt("abc", "123")) should be (false)
    p.equals(gt("col", "124")) should be (false)
    p.equals(gt("col", 123)) should be (false)
    p.equals(gt("col", 123L)) should be (false)
    p.hashCode should be (gt("col", "123").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == LessThan ==
  test("LessThan (int) - bound reference methods") {
    val p = lt("col", 123)
    p.equals(p) should be (true)
    p.equals(lt("col", 123)) should be (true)
    p.equals(lt("abc", 123)) should be (false)
    p.equals(lt("col", 124)) should be (false)
    p.equals(lt("col", 123L)) should be (false)
    p.equals(lt("col", "123")) should be (false)
    p.hashCode should be (lt("col", 123).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("LessThan (long) - bound reference methods") {
    val p = lt("col", 123L)
    p.equals(p) should be (true)
    p.equals(lt("col", 123L)) should be (true)
    p.equals(lt("abc", 123L)) should be (false)
    p.equals(lt("col", 124L)) should be (false)
    p.equals(lt("col", 123)) should be (false)
    p.equals(lt("col", "123")) should be (false)
    p.hashCode should be (lt("col", 123L).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("LessThan (UTF8) - bound reference methods") {
    val p = lt("col", "123")
    p.equals(p) should be (true)
    p.equals(lt("col", "123")) should be (true)
    p.equals(lt("abc", "123")) should be (false)
    p.equals(lt("col", "124")) should be (false)
    p.equals(lt("col", 123)) should be (false)
    p.equals(lt("col", 123L)) should be (false)
    p.hashCode should be (lt("col", "123").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == GreaterThanOrEqual ==
  test("GreaterThanOrEqual (int) - bound reference methods") {
    val p = ge("col", 123)
    p.equals(p) should be (true)
    p.equals(ge("col", 123)) should be (true)
    p.equals(ge("abc", 123)) should be (false)
    p.equals(ge("col", 124)) should be (false)
    p.equals(ge("col", 123L)) should be (false)
    p.equals(ge("col", "123")) should be (false)
    p.hashCode should be (ge("col", 123).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("GreaterThanOrEqual (long) - bound reference methods") {
    val p = ge("col", 123L)
    p.equals(p) should be (true)
    p.equals(ge("col", 123L)) should be (true)
    p.equals(ge("abc", 123L)) should be (false)
    p.equals(ge("col", 124L)) should be (false)
    p.equals(ge("col", 123)) should be (false)
    p.equals(ge("col", "123")) should be (false)
    p.hashCode should be (ge("col", 123L).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("GreaterThanOrEqual (UTF8) - bound reference methods") {
    val p = ge("col", "123")
    p.equals(p) should be (true)
    p.equals(ge("col", "123")) should be (true)
    p.equals(ge("abc", "123")) should be (false)
    p.equals(ge("col", "124")) should be (false)
    p.equals(ge("col", 123)) should be (false)
    p.equals(ge("col", 123L)) should be (false)
    p.hashCode should be (ge("col", "123").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == LessThanOrEqual ==
  test("LessThanOrEqual (int) - bound reference methods") {
    val p = le("col", 123)
    p.equals(p) should be (true)
    p.equals(le("col", 123)) should be (true)
    p.equals(le("abc", 123)) should be (false)
    p.equals(le("col", 124)) should be (false)
    p.equals(le("col", 123L)) should be (false)
    p.equals(le("col", "123")) should be (false)
    p.hashCode should be (le("col", 123).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("LessThanOrEqual (long) - bound reference methods") {
    val p = le("col", 123L)
    p.equals(p) should be (true)
    p.equals(le("col", 123L)) should be (true)
    p.equals(le("abc", 123L)) should be (false)
    p.equals(le("col", 124L)) should be (false)
    p.equals(le("col", 123)) should be (false)
    p.equals(le("col", "123")) should be (false)
    p.hashCode should be (le("col", 123L).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("LessThanOrEqual (UTF8) - bound reference methods") {
    val p = le("col", "123")
    p.equals(p) should be (true)
    p.equals(le("col", "123")) should be (true)
    p.equals(le("abc", "123")) should be (false)
    p.equals(le("col", "124")) should be (false)
    p.equals(le("col", 123)) should be (false)
    p.equals(le("col", 123L)) should be (false)
    p.hashCode should be (le("col", "123").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == In ==
  test("In (int[]) - bound reference methods") {
    val p = in("col", Array(1, 2, 3))
    p.hasMultiplValues should be (true)
    p.equals(p) should be (true)
    p.equals(in("col", Array(1, 2, 3))) should be (true)
    p.equals(in("col", Array(1L, 2L, 3L))) should be (false)
    p.equals(in("col", Array("1", "2", "3"))) should be (false)
    p.hashCode should be (in("col", Array(1, 2, 3)).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("In (long[]) - bound reference methods") {
    val p = in("col", Array(1L, 2L, 3L))
    p.hasMultiplValues should be (true)
    p.equals(p) should be (true)
    p.equals(in("col", Array(1L, 2L, 3L))) should be (true)
    p.equals(in("col", Array(1, 2, 3))) should be (false)
    p.equals(in("col", Array("1", "2", "3"))) should be (false)
    p.hashCode should be (in("col", Array(1L, 2L, 3L)).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  test("In (UTF8String[]) - bound reference methods") {
    val p = in("col", Array("1", "2", "3"))
    p.hasMultiplValues should be (true)
    p.equals(p) should be (true)
    p.equals(in("col", Array("1", "2", "3"))) should be (true)
    p.equals(in("col", Array(1L, 2L, 3L))) should be (false)
    p.equals(in("col", Array(1, 2, 3))) should be (false)
    p.hashCode should be (in("col", Array("1", "2", "3")).hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }

  // == IsNull ==
  test("IsNull - bound reference methods") {
    val p = nvl("col")
    p.equals(p) should be (true)
    p.equals(nvl("col")) should be (true)
    p.equals(nvl("col1")) should be (false)
    p.equals(eqt("col", "")) should be (false)
    p.hashCode should be (nvl("col").hashCode)

    val p1 = p.withOrdinal(100)
    assert(!(p1 eq p))
    p1.ordinal should be (100)
    p1.equals(p) should be (false)
  }
}
