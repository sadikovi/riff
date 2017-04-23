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
 * Represents leaf node in the tree, is always evaluable. Leaf node contains information about
 * ordinal position in the row and some value associated with specific implementation of this class.
 */
public abstract class LeafNode implements TreeNode {
  protected final int ordinal;
  // name is used to link field to type spec and resolve ordinal
  protected final String name;

  public LeafNode(String name, int ordinal) {
    this.name = name;
    this.ordinal = ordinal;
  }

  /**
   * Ordinal value of the corresponding type spec for which leaf node is created.
   * @return ordinal
   */
  public int ordinal() {
    return ordinal;
  }

  /**
   * Field name (optional), it is not used when evaluating predicate, but used for equality check,
   * and resolving node.
   * @return field name
   */
  public String name() {
    return name;
  }

  /**
   * This method is for evaluating row only. Use other special statistics methods to evaluate
   * predicate based on statistics.
   */
  @Override
  public final boolean evaluate(InternalRow row) {
    if (!resolved()) throw new IllegalStateException("Node " + this + " is not resolved");
    return evaluate(row, ordinal());
  }

  @Override
  public final boolean evaluate(Statistics[] stats) {
    if (!resolved()) throw new IllegalStateException("Node " + this + " is not resolved");
    return stats[ordinal()].evaluateState(this);
  }

  @Override
  public final boolean resolved() {
    // return true if ordinal is a valid array index
    return ordinal() >= 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) return false;
    LeafNode that = (LeafNode) obj;
    return name().equals(that.name()) && ordinal() == that.ordinal() &&
      value().equals(that.value());
  }

  @Override
  public int hashCode() {
    int result = 31 * ordinal + name.hashCode();
    result = result * 31 + value().hashCode();
    return 31 * result + getClass().hashCode();
  }

  @Override
  public String toString() {
    String tag = resolved() ? (name() + "[" + ordinal() + "]") : ("*" + name());
    return toString(tag);
  }

  /**
   * Get value for this leaf node.
   * Should not be used when evaluating tree.
   * @return value for leaf node
   */
  public abstract Object value();

  /**
   * Evaluate row for this ordinal.
   * This method is called after checks on predicate correctness and `resolved`.
   */
  protected abstract boolean evaluate(InternalRow row, int ordinal);

  /**
   * String method to return string representation based on pretty tag name.
   */
  protected abstract String toString(String tag);

  /**
   * Update node with statistics information about null values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   */
  public boolean statUpdate(boolean hasNulls) {
    return true;
  }

  /**
   * Update node with integer statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   */
  public boolean statUpdate(int min, int max) {
    return true;
  }

  /**
   * Update node with long statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   */
  public boolean statUpdate(long min, long max) {
    return true;
  }

  /**
   * Update node with UTF8String statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   */
  public boolean statUpdate(UTF8String min, UTF8String max) {
    return true;
  }
}
