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

package com.github.sadikovi.riff.ntree.expression;

import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.ntree.Rule;
import com.github.sadikovi.riff.ntree.Statistics;
import com.github.sadikovi.riff.ntree.Tree;
import com.github.sadikovi.riff.ntree.TypedBoundReference;
import com.github.sadikovi.riff.ntree.TypedExpression;

/**
 * [[In]] node represents predicate that evaluates row against a list of possible values.
 * Node has type information and support for statistics and column filter.
 */
public class In extends TypedBoundReference {
  private final String name;
  // sorted in ascending order list of typed expression
  // protected for equals and hashCode methods
  protected final TypedExpression[] list;

  public In(String name, TypedExpression[] values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("Empty list of expressions for In predicate");
    }
    this.name = name;
    this.list = new TypedExpression[values.length];
    // copy and sort expressions, check that all expressions are of the same type
    DataType dtype = null;
    for (int i = 0; i < values.length; i++) {
      if (dtype != null && !dtype.equals(values[i].dataType())) {
        throw new IllegalArgumentException("Invalid data type, expected " + dtype + ", found " +
          values[i].dataType());
      }
      dtype = values[i].dataType();
      this.list[i] = values[i].copy();
    }
    Arrays.sort(this.list);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public TypedExpression expression() {
    // list is always non-empty
    return list[0];
  }

  @Override
  public String operator() {
    return "in";
  }

  /**
   * Search array to find if there is a typed expression that matches ordinal value.
   * @param row row to evaluate
   * @param ordinal row ordinal
   * @return true if row value equals any typed expression, false otherwise
   */
  private boolean exists(InternalRow row, int ordinal) {
    int low = 0, high = list.length - 1;
    while (low <= high) {
      int mid = high - (high - low) / 2;
      if (list[mid].eqExpr(row, ordinal)) return true;
      if (list[mid].ltExpr(row, ordinal)) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return false;
  }

  @Override
  public boolean evaluateState(InternalRow row, int ordinal) {
    return !row.isNullAt(ordinal) && exists(row, ordinal);
  }

  @Override
  public boolean evaluateState(Statistics stats) {
    for (int i = 0; i < list.length; i++) {
      boolean contains =
        !stats.isNullAt(Statistics.ORD_MIN) &&
        !stats.isNullAt(Statistics.ORD_MAX) &&
        list[i].leExpr(stats, Statistics.ORD_MIN) &&
        list[i].geExpr(stats, Statistics.ORD_MAX);
      if (contains) return true;
    }
    return false;
  }

  @Override
  public boolean evaluateState(ColumnFilter filter) {
    for (int i = 0; i < list.length; i++) {
      if (list[i].containsExpr(filter)) return true;
    }
    return false;
  }

  @Override
  public Tree transform(Rule rule) {
    return rule.update(this);
  }

  @Override
  public Tree copy() {
    return new In(name, list).copyOrdinal(this);
  }

  @Override
  public boolean equals(Object obj) {
    // equals method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    if (obj == null || !(obj instanceof In)) return false;
    In that = (In) obj;
    return name().equals(that.name()) && ordinal() == that.ordinal() &&
      Arrays.equals(list, that.list);
  }

  @Override
  public int hashCode() {
    // hashCode method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    int result = 31 * ordinal() + name().hashCode();
    result = 31 * result + Arrays.hashCode(list);
    return 31 * result + getClass().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < list.length; i++) {
      sb.append(list[i].prettyString());
      if (i < list.length - 1) {
        sb.append(", ");
      }
    }
    sb.append("]");
    return prettyName() + " " + operator() + " " + sb.toString();
  }
}
