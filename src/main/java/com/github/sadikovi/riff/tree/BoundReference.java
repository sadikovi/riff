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
 * [[BoundReference]] represents leaf node with known name/ordinal and potentially value that this
 * name/ordinal binds to. This is a leaf node for filters, such as `EqualTo`, `GreaterThan`, etc.
 * Classes should inherit typed bound references if possible that provide typed value for each
 * implementation.
 */
public abstract class BoundReference implements TreeNode {
  protected final String name;
  protected final int ordinal;

  public BoundReference(String name, int ordinal) {
    this.name = name;
    this.ordinal = ordinal;
  }

  /**
   * Get name for reference.
   * @return name
   */
  public String name() {
    return name;
  }

  /**
   * Get ordinal for reference. Note that for unresolved node ordinal may not be valid.
   * @return ordinal
   */
  public int ordinal() {
    return ordinal;
  }

  /**
   * Return value as boxed object. This is only used for equals and hashCode methods and never
   * called during evaluation of the tree.
   * @return value
   */
  public abstract Object value();

  @Override
  public final boolean resolved() {
    // return true if ordinal is a valid array index
    return ordinal >= 0;
  }

  @Override
  public boolean equals(Object obj) {
    System.out.println("equals(" + obj + ")");
    // equals method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    if (obj == null || obj.getClass() != this.getClass()) return false;
    BoundReference that = (BoundReference) obj;
    if (!name.equals(that.name) || ordinal != that.ordinal) return false;
    return (value() == null && that.value() == null) ||
      (value() != null && that.value() != null && value().equals(that.value()));
  }

  @Override
  public int hashCode() {
    System.out.println("hashCode()");
    // hashCode method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    int result = 31 * ordinal + name.hashCode();
    result = result * 31 + (value() == null ? 0 : value().hashCode());
    return 31 * result + getClass().hashCode();
  }

  /**
   * Tree operator as string, for example. equality would return "=".
   * @return operator symbol
   */
  public abstract String treeOperator();

  @Override
  public final String toString() {
    String tag = resolved() ? (name + "[" + ordinal + "]") : ("*" + name);
    return tag + " " + treeOperator() + " " + value();
  }

  @Override
  public final boolean evaluate(Statistics[] stats) {
    // we do not enforce tree being resolved, instead the behaviour is undefined.
    // to enforce correct result, resolve tree first and check with `resolved()` method.
    return stats[ordinal].evaluateState(this);
  }

  /**
   * Update node with statistics information about null values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param hasNulls true if statistics has nulls, false otherwise
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(boolean hasNulls) {
    return true;
  }

  /**
   * Update node with integer statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min int value
   * @param max max int value
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(int min, int max) {
    return true;
  }

  /**
   * Update node with long statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min long value
   * @param max max long value
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(long min, long max) {
    return true;
  }

  /**
   * Update node with UTF8String statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min UTF8 value or null
   * @param max max UTF8 value or null
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(UTF8String min, UTF8String max) {
    return true;
  }
}
