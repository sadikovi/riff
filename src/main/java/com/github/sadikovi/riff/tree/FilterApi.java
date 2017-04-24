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

package com.github.sadikovi.riff.tree;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.tree.Tree.And;
import com.github.sadikovi.riff.tree.Tree.GreaterThan;
import com.github.sadikovi.riff.tree.Tree.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.Tree.EqualTo;
import com.github.sadikovi.riff.tree.Tree.IsNull;
import com.github.sadikovi.riff.tree.Tree.LessThan;
import com.github.sadikovi.riff.tree.Tree.LessThanOrEqual;
import com.github.sadikovi.riff.tree.Tree.Not;
import com.github.sadikovi.riff.tree.Tree.Or;
import com.github.sadikovi.riff.tree.Tree.Trivial;

/**
 * [[FilterApi]] is used to construct predicate tree. It is recommended to use this set of methods
 * rather directly instantiating tree node classes.
 * Usage:
 * {{{
 * > import FilterApi.*;
 * > TreeNode tree = or(and(eqt("a", 1), eqt("b", 10L)), not(nvl("c")));
 * t: com.github.sadikovi.riff.tree.TreeNode = ((*a = 1) && (*b = 10)) || (!(*c is null))
 * }}}
 * It is assumed that all nodes are non-null, but checks are in place for assertion. If value is
 * null, use `nvl` method instead of passing null reference.
 */
public class FilterApi {
  private FilterApi() { }

  // default unresolved ordinal
  private static final int ORD = -1;

  //////////////////////////////////////////////////////////////
  // EqualTo
  //////////////////////////////////////////////////////////////

  /**
   * Create equality filter for integer value.
   * @param name field name
   * @param value filter value
   * @return EQT(field, int)
   */
  public static TreeNode eqt(String name, final int value) {
    return new EqualTo(name, ORD) {
      @Override
      public Object value() {
        return value;
      }

      @Override
      public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) == value;
      }

      @Override
      public boolean statUpdate(int min, int max) {
        return min >= value && value <= max;
      }
    };
  }

  /**
   * Create equality filter for long value.
   * @param name field name
   * @param value filter value
   * @return EQT(field, long)
   */
  public static TreeNode eqt(String name, final long value) {
    return new EqualTo(name, ORD) {
      @Override
      public Object value() {
        return value;
      }

      @Override
      public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) == value;
      }

      @Override
      public boolean statUpdate(long min, long max) {
        return min >= value && value <= max;
      }
    };
  }

  /**
   * Create equality filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return EQT(field, UTF8String)
   */
  public static TreeNode eqt(String name, final UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return new EqualTo(name, ORD) {
      @Override
      public Object value() {
        return value;
      }

      @Override
      public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && value.equals(row.getUTF8String(ordinal));
      }

      @Override
      public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return min.compareTo(value) >= 0 && value.compareTo(max) <= 0;
      }
    };
  }

  /**
   * Create equality filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return EQT(field, UTF8String)
   */
  public static TreeNode eqt(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return eqt(name, UTF8String.fromString(value));
  }

  //////////////////////////////////////////////////////////////
  // IsNull
  //////////////////////////////////////////////////////////////

  /**
   * Create `IsNull` filter for a field, where filter value is null
   * @param name field name
   * @return IS_NULL(field)
   */
  public static TreeNode nvl(String name) {
    return new IsNull(name, ORD);
  }

  //////////////////////////////////////////////////////////////
  // Logical filters
  //////////////////////////////////////////////////////////////

  /**
   * Create logical AND between 2 child filters.
   * @param left left filter
   * @param right right filter
   * @return AND(left, right)
   */
  public static TreeNode and(TreeNode left, TreeNode right) {
    if (left == null || right == null) throw new IllegalArgumentException("Child node is null");
    return new And(left, right);
  }

  /**
   * Create logical OR between 2 child filters.
   * @param left left filter
   * @param right right filter
   * @return OR(left, right)
   */
  public static TreeNode or(TreeNode left, TreeNode right) {
    if (left == null || right == null) throw new IllegalArgumentException("Child node is null");
    return new Or(left, right);
  }

  /**
   * Create negation of the child filter.
   * @param child child filter
   * @return NOT(child)
   */
  public static TreeNode not(TreeNode child) {
    if (child == null) throw new IllegalArgumentException("Child node is null");
    return new Not(child);
  }

  //////////////////////////////////////////////////////////////
  // Literals
  //////////////////////////////////////////////////////////////

  /**
   * Create trivial `true` filter.
   * @return TRUE
   */
  public static TreeNode TRUE() {
    return new Trivial(true);
  }

  /**
   * Create trivial `false` filter.
   * @return FALSE
   */
  public static TreeNode FALSE() {
    return new Trivial(false);
  }
}
