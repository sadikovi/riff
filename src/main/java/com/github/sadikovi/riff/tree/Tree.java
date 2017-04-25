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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.Statistics;

public class Tree {
  private Tree() { }

  /**
   * "EqualTo" leaf node, equality for row ordinal and provided value.
   */
  public static abstract class EqualTo extends BoundReference {
    public EqualTo(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public String treeOperator() {
      return "=";
    }
  }

  /**
   * "GreaterThan" leaf node, ordinal row value is greater than provided value.
   */
  public static abstract class GreaterThan extends BoundReference {
    public GreaterThan(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public String treeOperator() {
      return ">";
    }
  }

  /**
   * "LessThan" leaf node, ordinal row value is less than provided value.
   */
  public static abstract class LessThan extends BoundReference {
    public LessThan(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public String treeOperator() {
      return "<";
    }
  }

  /**
   * "GreaterThanOrEqual" leaf node, ordinal row value is greater than or equal to provided value.
   */
  public static abstract class GreaterThanOrEqual extends BoundReference {
    public GreaterThanOrEqual(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public String treeOperator() {
      return ">=";
    }
  }

  /**
   * "LessThanOrEqual" leaf node, ordinal row value is less than or equal to provided value.
   */
  public static abstract class LessThanOrEqual extends BoundReference {
    public LessThanOrEqual(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public String treeOperator() {
      return "<=";
    }
  }

  /**
   * "In" leaf node, ordinal row value is in provided list of values.
   * In filter is backed by sorted array.
   * TODO: replace with typed hash table.
   */
  public static abstract class In extends BoundReference {
    public In(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public final boolean hasMultiplValues() {
      return true;
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public String treeOperator() {
      return "isin";
    }
  }

  /**
   * "IsNull" leaf node, indicates that value for the field is null.
   */
  public static class IsNull extends BoundReference {
    public IsNull(String name, int ordinal) {
      super(name, ordinal);
    }

    @Override
    public boolean evaluate(InternalRow row) {
      return row.isNullAt(ordinal);
    }

    @Override
    public boolean statUpdate(boolean hasNulls) {
      // this filter represents search for a value for this field which is null,
      // if statistics does not have nulls, we should return false
      return hasNulls;
    }

    @Override
    public TreeNode transform(Modifier modifier) {
      return modifier.update(this);
    }

    @Override
    public DataType dataType() {
      return DataTypes.NullType;
    }

    @Override
    public final boolean hasMultiplValues() {
      return false;
    }

    @Override
    public IsNull withOrdinal(int newOrdinal) {
      return new IsNull(name, newOrdinal);
    }

    @Override
    public String treeOperator() {
      return "is";
    }
  }

  /**
   * "And" binary logical node, intersection of left and right children.
   */
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
      return "(" + left + ") && (" + right + ")";
    }
  }


  /**
   * "Or" binary logical node, union of left and right children.
   */
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
      return "(" + left + ") || (" + right + ")";
    }
  }

  /**
   * "Not" unary logical node, negates expression of child.
   */
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

  /**
   * Trivial node, that is already evaluated.
   */
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
      if (obj == null || !(obj instanceof Trivial)) return false;
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
