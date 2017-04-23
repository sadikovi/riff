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

import com.github.sadikovi.riff.Statistics;

/**
 * Set of concrete implementations for TreeNode interface and subclasses.
 * Should not be used directly to construct tree, use FilterAPI instead.
 */
public class Tree {
  private Tree() { }

  //////////////////////////////////////////////////////////////
  // Equality filter (see concrete implementations for different value types)
  //////////////////////////////////////////////////////////////

  public static abstract class Eq extends LeafNode {
    public Eq(String name, int ordinal) {
      super(name, ordinal);
    }

    /**
     * Get value for this equality filter.
     * Should not be used when evaluating tree.
     * @return value for this filter
     */
    public abstract Object value();

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != this.getClass()) return false;
      Eq that = (Eq) obj;
      return name().equals(that.name()) && ordinal() == that.ordinal() &&
        value().equals(that.value());
    }

    @Override
    public int hashCode() {
      int result = 31 * ordinal + name.hashCode();
      return result * 31 + value().hashCode();
    }

    @Override
    protected String toString(String tag) {
      return tag + " = " + value();
    }
  }

  static class EqInt extends Eq {
    private final int value;

    EqInt(String name, int ordinal, int value) {
      super(name, ordinal);
      this.value = value;
    }

    @Override
    public Object value() {
      return value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getInt(ordinal) == value;
    }

    @Override
    public boolean statUpdate(int min, int max) {
      return min >= value && value <= max;
    }
  }

  static class EqLong extends Eq {
    private final long value;

    EqLong(String name, int ordinal, long value) {
      super(name, ordinal);
      this.value = value;
    }

    @Override
    public Object value() {
      return value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getLong(ordinal) == value;
    }

    @Override
    public boolean statUpdate(long min, long max) {
      return min >= value && value <= max;
    }
  }

  static class EqUTF8String extends Eq {
    private final UTF8String value;

    EqUTF8String(String name, int ordinal, UTF8String value) {
      super(name, ordinal);
      if (value == null) throw new IllegalArgumentException("For null values use IsNull node");
      this.value = value;
    }

    @Override
    public Object value() {
      return value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getUTF8String(ordinal).equals(value);
    }

    @Override
    public boolean statUpdate(UTF8String min, UTF8String max) {
      if (min == null && max == null) return false;
      return value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
    }
  }

  // Leaf node when value is null for an ordinal
  public static class IsNull extends LeafNode {
    public IsNull(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.isNullAt(ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public boolean statUpdate(boolean hasNulls) {
      // this filter represents search for a value for this field which is null,
      // if statistics does not have nulls, we should return false
      return hasNulls;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != this.getClass()) return false;
      IsNull that = (IsNull) obj;
      return that.ordinal() == ordinal() && name().equals(that.name());
    }

    @Override
    public int hashCode() {
      return 31 * ordinal + name.hashCode();
    }

    @Override
    protected String toString(String tag) {
      return tag + " is null";
    }
  }

  //////////////////////////////////////////////////////////////
  // "And" filter, logical intersection of child expressions
  //////////////////////////////////////////////////////////////

  public static class And extends BinaryLogicalNode {
    private final TreeNode left;
    private final TreeNode right;

    public And(TreeNode left, TreeNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public TreeNode left() {
      return left;
    }

    @Override
    public TreeNode right() {
      return right;
    }

    @Override
    public boolean evaluate(InternalRow row) {
      return left.evaluate(row) && right.evaluate(row);
    }

    @Override
    public boolean evaluate(Statistics[] stats) {
      return left.evaluate(stats) && right.evaluate(stats);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public String toString() {
      return "(" + left + " && " + right + ")";
    }
  }

  //////////////////////////////////////////////////////////////
  // "Or" filter, logical union of child expressions
  //////////////////////////////////////////////////////////////

  public static class Or extends BinaryLogicalNode {
    private final TreeNode left;
    private final TreeNode right;

    public Or(TreeNode left, TreeNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public TreeNode left() {
      return left;
    }

    @Override
    public TreeNode right() {
      return right;
    }

    @Override
    public boolean evaluate(InternalRow row) {
      return left.evaluate(row) || right.evaluate(row);
    }

    @Override
    public boolean evaluate(Statistics[] stats) {
      return left.evaluate(stats) || right.evaluate(stats);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public String toString() {
      return "(" + left + " || " + right + ")";
    }
  }

  //////////////////////////////////////////////////////////////
  // "Not" filter, negates child expression
  //////////////////////////////////////////////////////////////

  public static class Not extends UnaryLogicalNode {
    private final TreeNode child;

    public Not(TreeNode child) {
      this.child = child;
    }

    @Override
    public TreeNode child() {
      return child;
    }

    @Override
    public boolean evaluate(InternalRow row) {
      return !child.evaluate(row);
    }

    @Override
    public boolean evaluate(Statistics[] stats) {
      return !child.evaluate(stats);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public String toString() {
      return "!(" + child + ")";
    }
  }

  //////////////////////////////////////////////////////////////
  // Filter representing trivial already resolved value
  //////////////////////////////////////////////////////////////

  public static class Trivial implements TreeNode {
    private final boolean result;

    public Trivial(boolean result) {
      this.result = result;
    }

    /**
     * Return result for this trivial filter.
     * @return true or false
     */
    public boolean result() {
      return result;
    }

    @Override
    public boolean evaluate(InternalRow row) {
      return result;
    }

    @Override
    public boolean evaluate(Statistics[] stats) {
      return result;
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public boolean resolved() {
      // trivial predicate is always resolved, since result is known
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != this.getClass()) return false;
      Trivial that = (Trivial) obj;
      return that.result() == this.result();
    }

    @Override
    public int hashCode() {
      return 31 + (result ? 1 : 0);
    }

    @Override
    public String toString() {
      return "" + result;
    }
  }
}
