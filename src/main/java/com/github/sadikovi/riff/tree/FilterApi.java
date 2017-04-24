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
  private static final int UNRESOLVED_ORDINAL = -1;

  //////////////////////////////////////////////////////////////
  // EqualTo
  //////////////////////////////////////////////////////////////

  /**
   * Create equality filter for integer value.
   * @param name field name
   * @param value filter value
   * @return EQT(field, int)
   */
  public static EqualTo eqt(String name, int value) {
    return eqt(name, UNRESOLVED_ORDINAL, value);
  }

  private static EqualTo eqt(String name, int ordinal, final int value) {
    return new EqualTo(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) == value;
      }

      @Override public EqualTo withOrdinal(int newOrdinal) {
        return eqt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(int min, int max) {
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
  public static EqualTo eqt(String name, long value) {
    return eqt(name, UNRESOLVED_ORDINAL, value);
  }

  private static EqualTo eqt(String name, int ordinal, final long value) {
    return new EqualTo(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) == value;
      }

      @Override public EqualTo withOrdinal(int newOrdinal) {
        return eqt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(long min, long max) {
        return min >= value && value <= max;
      }
    };
  }

  /**
   * Create equality filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return EQT(field, UTF8String)
   */
  public static EqualTo eqt(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return eqt(name, UTF8String.fromString(value));
  }

  /**
   * Create equality filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return EQT(field, UTF8String)
   */
  public static EqualTo eqt(String name, UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return eqt(name, UNRESOLVED_ORDINAL, value);
  }

  private static EqualTo eqt(String name, int ordinal, final UTF8String value) {
    return new EqualTo(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getUTF8String(ordinal).equals(value);
      }

      @Override public EqualTo withOrdinal(int newOrdinal) {
        return eqt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return min.compareTo(value) >= 0 && value.compareTo(max) <= 0;
      }
    };
  }

  //////////////////////////////////////////////////////////////
  // GreaterThan
  //////////////////////////////////////////////////////////////

  /**
   * Create '>' filter for integer value.
   * @param name field name
   * @param value filter value
   * @return GT(field, int)
   */
  public static GreaterThan gt(String name, int value) {
    return gt(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThan gt(String name, int ordinal, final int value) {
    return new GreaterThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) > value;
      }

      @Override public GreaterThan withOrdinal(int newOrdinal) {
        return gt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(int min, int max) {
        return value < max;
      }
    };
  }

  /**
   * Create '>' filter for long value.
   * @param name field name
   * @param value filter value
   * @return GT(field, long)
   */
  public static GreaterThan gt(String name, long value) {
    return gt(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThan gt(String name, int ordinal, final long value) {
    return new GreaterThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) > value;
      }

      @Override public GreaterThan withOrdinal(int newOrdinal) {
        return gt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(long min, long max) {
        return value < max;
      }
    };
  }

  /**
   * Create '>' filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return GT(field, UTF8String)
   */
  public static GreaterThan gt(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return gt(name, UTF8String.fromString(value));
  }

  /**
   * Create '>' filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return GT(field, UTF8String)
   */
  public static GreaterThan gt(String name, UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return gt(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThan gt(String name, int ordinal, final UTF8String value) {
    return new GreaterThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getUTF8String(ordinal).compareTo(value) > 0;
      }

      @Override public GreaterThan withOrdinal(int newOrdinal) {
        return gt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return value.compareTo(max) < 0;
      }
    };
  }

  //////////////////////////////////////////////////////////////
  // LessThan
  //////////////////////////////////////////////////////////////

  /**
   * Create '<' filter for integer value.
   * @param name field name
   * @param value filter value
   * @return LT(field, int)
   */
  public static LessThan lt(String name, int value) {
    return lt(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThan lt(String name, int ordinal, final int value) {
    return new LessThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) < value;
      }

      @Override public LessThan withOrdinal(int newOrdinal) {
        return lt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(int min, int max) {
        return value > min;
      }
    };
  }

  /**
   * Create '<' filter for long value.
   * @param name field name
   * @param value filter value
   * @return LT(field, long)
   */
  public static LessThan lt(String name, long value) {
    return lt(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThan lt(String name, int ordinal, final long value) {
    return new LessThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) < value;
      }

      @Override public LessThan withOrdinal(int newOrdinal) {
        return lt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(long min, long max) {
        return value > min;
      }
    };
  }

  /**
   * Create '<' filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return LT(field, UTF8String)
   */
  public static LessThan lt(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return lt(name, UTF8String.fromString(value));
  }

  /**
   * Create '<' filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return LT(field, UTF8String)
   */
  public static LessThan lt(String name, UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return lt(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThan lt(String name, int ordinal, final UTF8String value) {
    return new LessThan(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getUTF8String(ordinal).compareTo(value) < 0;
      }

      @Override public LessThan withOrdinal(int newOrdinal) {
        return lt(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return value.compareTo(min) > 0;
      }
    };
  }

  //////////////////////////////////////////////////////////////
  // GreaterThanOrEqual
  //////////////////////////////////////////////////////////////

  /**
   * Create '>=' filter for integer value.
   * @param name field name
   * @param value filter value
   * @return GE(field, int)
   */
  public static GreaterThanOrEqual ge(String name, int value) {
    return ge(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThanOrEqual ge(String name, int ordinal, final int value) {
    return new GreaterThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) >= value;
      }

      @Override public GreaterThanOrEqual withOrdinal(int newOrdinal) {
        return ge(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(int min, int max) {
        return value <= max;
      }
    };
  }

  /**
   * Create '>=' filter for long value.
   * @param name field name
   * @param value filter value
   * @return GE(field, long)
   */
  public static GreaterThanOrEqual ge(String name, long value) {
    return ge(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThanOrEqual ge(String name, int ordinal, final long value) {
    return new GreaterThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) >= value;
      }

      @Override public GreaterThanOrEqual withOrdinal(int newOrdinal) {
        return ge(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(long min, long max) {
        return value <= max;
      }
    };
  }

  /**
   * Create '>=' filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return GE(field, UTF8String)
   */
  public static GreaterThanOrEqual ge(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return ge(name, UTF8String.fromString(value));
  }

  /**
   * Create '>=' filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return GE(field, UTF8String)
   */
  public static GreaterThanOrEqual ge(String name, UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return ge(name, UNRESOLVED_ORDINAL, value);
  }

  private static GreaterThanOrEqual ge(String name, int ordinal, final UTF8String value) {
    return new GreaterThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getUTF8String(ordinal).compareTo(value) >= 0;
      }

      @Override public GreaterThanOrEqual withOrdinal(int newOrdinal) {
        return ge(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return value.compareTo(max) <= 0;
      }
    };
  }

  //////////////////////////////////////////////////////////////
  // LessThanOrEqual
  //////////////////////////////////////////////////////////////

  /**
   * Create '<=' filter for integer value.
   * @param name field name
   * @param value filter value
   * @return LE(field, int)
   */
  public static LessThanOrEqual le(String name, int value) {
    return le(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThanOrEqual le(String name, int ordinal, final int value) {
    return new LessThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getInt(ordinal) <= value;
      }

      @Override public LessThanOrEqual withOrdinal(int newOrdinal) {
        return le(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(int min, int max) {
        return value >= min;
      }
    };
  }

  /**
   * Create '<=' filter for long value.
   * @param name field name
   * @param value filter value
   * @return LE(field, long)
   */
  public static LessThanOrEqual le(String name, long value) {
    return le(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThanOrEqual le(String name, int ordinal, final long value) {
    return new LessThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getLong(ordinal) <= value;
      }

      @Override public LessThanOrEqual withOrdinal(int newOrdinal) {
        return le(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(long min, long max) {
        return value >= min;
      }
    };
  }

  /**
   * Create '<=' filter for String value; value is converted into UTF8String.
   * @param name field name
   * @param value filter value
   * @return LE(field, UTF8String)
   */
  public static LessThanOrEqual le(String name, String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return le(name, UTF8String.fromString(value));
  }

  /**
   * Create '<=' filter for UTF8String value.
   * @param name field name
   * @param value filter value
   * @return LE(field, UTF8String)
   */
  public static LessThanOrEqual le(String name, UTF8String value) {
    if (value == null) throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    return le(name, UNRESOLVED_ORDINAL, value);
  }

  private static LessThanOrEqual le(String name, int ordinal, final UTF8String value) {
    return new LessThanOrEqual(name, ordinal) {
      @Override public Object value() {
        return value;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && row.getUTF8String(ordinal).compareTo(value) <= 0;
      }

      @Override public LessThanOrEqual withOrdinal(int newOrdinal) {
        return le(name, newOrdinal, value);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        return value.compareTo(min) >= 0;
      }
    };
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
    return new IsNull(name, UNRESOLVED_ORDINAL);
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
