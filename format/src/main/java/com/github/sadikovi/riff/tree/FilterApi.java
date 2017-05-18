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

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.tree.expression.DateExpression;
import com.github.sadikovi.riff.tree.expression.IntegerExpression;
import com.github.sadikovi.riff.tree.expression.LongExpression;
import com.github.sadikovi.riff.tree.expression.TimestampExpression;
import com.github.sadikovi.riff.tree.expression.UTF8StringExpression;

import com.github.sadikovi.riff.tree.expression.EqualTo;
import com.github.sadikovi.riff.tree.expression.GreaterThan;
import com.github.sadikovi.riff.tree.expression.LessThan;
import com.github.sadikovi.riff.tree.expression.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.expression.LessThanOrEqual;
import com.github.sadikovi.riff.tree.expression.In;
import com.github.sadikovi.riff.tree.expression.IsNull;
import com.github.sadikovi.riff.tree.expression.Not;
import com.github.sadikovi.riff.tree.expression.And;
import com.github.sadikovi.riff.tree.expression.Or;
import com.github.sadikovi.riff.tree.expression.Trivial;

/**
 * [[FilterApi]] is used to construct predicate tree. It is recommended to use this set of methods
 * rather directly instantiating tree node classes.
 * Usage:
 * {{{
 * > import FilterApi.*;
 * > Tree tree = or(and(eqt("a", 1), eqt("b", 10L)), not(nvl("c")));
 * t: com.github.sadikovi.riff.tree.Tree = ((*a = 1) && (*b = 10)) || (!(*c is null))
 * }}}
 * It is assumed that all nodes are non-null, but checks are in place for assertion. If value is
 * null, use `nvl` method instead of passing null reference.
 */
public class FilterApi {
  private FilterApi() { }

  /**
   * Method to convert object into typed expression, throws exception if no match is found
   * @param obj object to convert, must never be null
   * @return typed expression for this object
   */
  protected static TypedExpression objToExpression(Object obj) {
    if (obj == null) {
      throw new NullPointerException("Cannot convert null into typed expression");
    }

    if (obj instanceof Integer) {
      return new IntegerExpression((Integer) obj);
    } else if (obj instanceof Long) {
      return new LongExpression((Long) obj);
    } else if (obj instanceof String) {
      return new UTF8StringExpression(UTF8String.fromString((String) obj));
    } else if (obj instanceof UTF8String) {
      return new UTF8StringExpression((UTF8String) obj);
    } else if (obj instanceof Date) {
      return new DateExpression((Date) obj);
    } else if (obj instanceof Timestamp) {
      return new TimestampExpression((Timestamp) obj);
    } else {
      throw new UnsupportedOperationException("Object " + obj + " of " + obj.getClass());
    }
  }

  // `true` predicate
  public static final Trivial TRUE = new Trivial(true);

  // `false` predicate
  public static final Trivial FALSE = new Trivial(false);

  /**
   * Create "equal to" filter.
   * @param columnName column name
   * @param value value
   * @return EqualTo(column, value)
   */
  public static EqualTo eqt(String columnName, Object value) {
    return new EqualTo(columnName, objToExpression(value));
  }

  /**
   * Create "greater than" filter.
   * @param columnName column name
   * @param value value
   * @return GreaterThan(column, value)
   */
  public static GreaterThan gt(String columnName, Object value) {
    return new GreaterThan(columnName, objToExpression(value));
  }

  /**
   * Create "less than" filter.
   * @param columnName column name
   * @param value value
   * @return LessThan(column, value)
   */
  public static LessThan lt(String columnName, Object value) {
    return new LessThan(columnName, objToExpression(value));
  }

  /**
   * Create greater than or equalt o filter.
   * @param columnName column name
   * @param value value
   * @return GreaterThanOrEqual(column, value)
   */
  public static GreaterThanOrEqual ge(String columnName, Object value) {
    return new GreaterThanOrEqual(columnName, objToExpression(value));
  }

  /**
   * Create "less than or equal to" filter.
   * @param columnName column name
   * @param value value
   * @return LessThanOrEqual(column, value)
   */
  public static LessThanOrEqual le(String columnName, Object value) {
    return new LessThanOrEqual(columnName, objToExpression(value));
  }

  /**
   * Create "in" filter.
   * @param columnName column name
   * @param value value
   * @return In(column, value)
   */
  public static In in(String columnName, Object... values) {
    TypedExpression[] expressions = new TypedExpression[values.length];
    for (int i = 0; i < values.length; i++) {
      expressions[i] = objToExpression(values[i]);
    }
    return new In(columnName, expressions);
  }

  /**
   * Create "is null" filter.
   * @param columnName column name
   * @param value value
   * @return EqualTo(column, value)
   */
  public static IsNull nvl(String columnName) {
    return new IsNull(columnName);
  }

  /**
   * Create "not" filter.
   * @param columnName column name
   * @param value value
   * @return Not(child tree)
   */
  public static Not not(Tree child) {
    return new Not(child);
  }

  /**
   * Create "and" filter.
   * @param columnName column name
   * @param value value
   * @return And(left subtree, right subtree)
   */
  public static And and(Tree left, Tree right) {
    return new And(left, right);
  }

  /**
   * Create "or" filter.
   * @param columnName column name
   * @param value value
   * @return Or(left subtree, right subtree)
   */
  public static Or or(Tree left, Tree right) {
    return new Or(left, right);
  }
}
