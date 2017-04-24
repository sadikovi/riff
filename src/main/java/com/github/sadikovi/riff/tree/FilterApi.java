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

import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.tree.Tree.And;
import com.github.sadikovi.riff.tree.Tree.GreaterThan;
import com.github.sadikovi.riff.tree.Tree.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.Tree.EqualTo;
import com.github.sadikovi.riff.tree.Tree.In;
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

  /**
   * Create equality filter.
   * @param name field name
   * @param value filter value
   * @return EQT(field, value)
   */
  public static EqualTo eqt(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof Integer) {
      return eqt(name, UNRESOLVED_ORDINAL, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      return eqt(name, UNRESOLVED_ORDINAL, ((Long) value).longValue());
    } else if (value instanceof UTF8String) {
      return eqt(name, UNRESOLVED_ORDINAL, (UTF8String) value);
    } else if (value instanceof String) {
      return eqt(name, UNRESOLVED_ORDINAL, UTF8String.fromString((String) value));
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  /**
   * Create '>' filter.
   * @param name field name
   * @param value filter value
   * @return GT(field, value)
   */
  public static GreaterThan gt(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof Integer) {
      return gt(name, UNRESOLVED_ORDINAL, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      return gt(name, UNRESOLVED_ORDINAL, ((Long) value).longValue());
    } else if (value instanceof UTF8String) {
      return gt(name, UNRESOLVED_ORDINAL, (UTF8String) value);
    } else if (value instanceof String) {
      return gt(name, UNRESOLVED_ORDINAL, UTF8String.fromString((String) value));
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  /**
   * Create '<' filter.
   * @param name field name
   * @param value filter value
   * @return LT(field, value)
   */
  public static LessThan lt(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof Integer) {
      return lt(name, UNRESOLVED_ORDINAL, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      return lt(name, UNRESOLVED_ORDINAL, ((Long) value).longValue());
    } else if (value instanceof UTF8String) {
      return lt(name, UNRESOLVED_ORDINAL, (UTF8String) value);
    } else if (value instanceof String) {
      return lt(name, UNRESOLVED_ORDINAL, UTF8String.fromString((String) value));
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  /**
   * Create '>=' filter.
   * @param name field name
   * @param value filter value
   * @return GE(field, value)
   */
  public static GreaterThanOrEqual ge(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof Integer) {
      return ge(name, UNRESOLVED_ORDINAL, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      return ge(name, UNRESOLVED_ORDINAL, ((Long) value).longValue());
    } else if (value instanceof UTF8String) {
      return ge(name, UNRESOLVED_ORDINAL, (UTF8String) value);
    } else if (value instanceof String) {
      return ge(name, UNRESOLVED_ORDINAL, UTF8String.fromString((String) value));
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  /**
   * Create '<=' filter.
   * @param name field name
   * @param value filter value
   * @return LE(field, value)
   */
  public static LessThanOrEqual le(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof Integer) {
      return le(name, UNRESOLVED_ORDINAL, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      return le(name, UNRESOLVED_ORDINAL, ((Long) value).longValue());
    } else if (value instanceof UTF8String) {
      return le(name, UNRESOLVED_ORDINAL, (UTF8String) value);
    } else if (value instanceof String) {
      return le(name, UNRESOLVED_ORDINAL, UTF8String.fromString((String) value));
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  /**
   * Create 'isin' filter.
   * @param name field name
   * @param value filter value
   * @return IN(field, value)
   */
  public static In in(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null. Use `IsNull` instead");
    } else if (value instanceof int[]) {
      return in(name, UNRESOLVED_ORDINAL, (int[]) value);
    } else if (value instanceof long[]) {
      return in(name, UNRESOLVED_ORDINAL, (long[]) value);
    } else if (value instanceof UTF8String[]) {
      return in(name, UNRESOLVED_ORDINAL, (UTF8String[]) value);
    } else if (value instanceof String[]) {
      String[] arr0 = (String[]) value;
      UTF8String[] arr1 = new UTF8String[arr0.length];
      for (int i = 0; i < arr0.length; i++) {
        arr1[i] = UTF8String.fromString(arr0[i]);
      }
      return in(name, UNRESOLVED_ORDINAL, arr1);
    } else {
      throw new UnsupportedOperationException("No filter registered for value " + value +
        " of type " + value.getClass());
    }
  }

  //////////////////////////////////////////////////////////////
  // EqualTo
  //////////////////////////////////////////////////////////////

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
  // In
  //////////////////////////////////////////////////////////////

  private static In in(String name, int ordinal, int[] value) {
    final int[] arr = new int[value.length];
    System.arraycopy(value, 0, arr, 0, value.length);
    // sort in ascending order
    Arrays.sort(arr);

    return new In(name, ordinal) {
      @Override public Object value() {
        return arr;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && Arrays.binarySearch(arr, row.getInt(ordinal)) >= 0;
      }

      @Override public In withOrdinal(int newOrdinal) {
        return in(name, newOrdinal, arr);
      }

      @Override public boolean statUpdate(int min, int max) {
        for (int i = 0; i < arr.length; i++) {
          if (arr[i] < min || arr[i] > max) return false;
        }
        return true;
      }
    };
  }

  private static In in(String name, int ordinal, long[] value) {
    final long[] arr = new long[value.length];
    System.arraycopy(value, 0, arr, 0, value.length);
    // sort in ascending order
    Arrays.sort(arr);

    return new In(name, ordinal) {
      @Override public Object value() {
        return arr;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && Arrays.binarySearch(arr, row.getLong(ordinal)) >= 0;
      }

      @Override public In withOrdinal(int newOrdinal) {
        return in(name, newOrdinal, arr);
      }

      @Override public boolean statUpdate(long min, long max) {
        for (int i = 0; i < arr.length; i++) {
          if (arr[i] < min || arr[i] > max) return false;
        }
        return true;
      }
    };
  }

  private static In in(String name, int ordinal, final UTF8String[] value) {
    final UTF8String[] arr = new UTF8String[value.length];
    System.arraycopy(value, 0, arr, 0, value.length);
    // sort in ascending order
    Arrays.sort(arr);

    return new In(name, ordinal) {
      @Override public Object value() {
        return arr;
      }

      @Override public boolean evaluate(InternalRow row) {
        return !row.isNullAt(ordinal) && Arrays.binarySearch(arr, row.getUTF8String(ordinal)) >= 0;
      }

      @Override public In withOrdinal(int newOrdinal) {
        return in(name, newOrdinal, arr);
      }

      @Override public boolean statUpdate(UTF8String min, UTF8String max) {
        if (min == null && max == null) return false;
        for (int i = 0; i < arr.length; i++) {
          if (arr[i].compareTo(min) < 0 || arr[i].compareTo(max) > 0) return false;
        }
        return true;
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
