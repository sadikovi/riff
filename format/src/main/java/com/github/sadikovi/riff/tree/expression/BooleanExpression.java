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

package com.github.sadikovi.riff.tree.expression;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.github.sadikovi.riff.column.ColumnFilter;
import com.github.sadikovi.riff.tree.TypedExpression;

/**
 * [[BooleanExpression]] to hold boolean value that used for comparison for all typed bound
 * references.
 */
public class BooleanExpression implements TypedExpression<BooleanExpression> {
  // value is accessible to equality and comparison methods
  protected final boolean value;

  public BooleanExpression(boolean value) {
    this.value = value;
  }

  @Override
  public DataType dataType() {
    return DataTypes.BooleanType;
  }

  @Override
  public boolean eqExpr(InternalRow row, int ordinal) {
    return row.getBoolean(ordinal) == value;
  }

  @Override
  public boolean gtExpr(InternalRow row, int ordinal) {
    // value is greater than expression only if row ordinal is true and expression is false
    return row.getBoolean(ordinal) && !value;
  }

  @Override
  public boolean ltExpr(InternalRow row, int ordinal) {
    // inverse of previous expression, row ordinal is false and expression is true
    return !row.getBoolean(ordinal) && value;
  }

  @Override
  public boolean geExpr(InternalRow row, int ordinal) {
    return eqExpr(row, ordinal) || gtExpr(row, ordinal);
  }

  @Override
  public boolean leExpr(InternalRow row, int ordinal) {
    return eqExpr(row, ordinal) || ltExpr(row, ordinal);
  }

  @Override
  public boolean containsExpr(ColumnFilter filter) {
    return filter.mightContain(value);
  }

  @Override
  public int compareTo(BooleanExpression obj) {
    if (!value && obj.value) {
      return -1;
    } else if (value && !obj.value) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof BooleanExpression)) return false;
    BooleanExpression expr = (BooleanExpression) obj;
    return expr.value == value;
  }

  @Override
  public int hashCode() {
    return value ? 1 : 0;
  }

  @Override
  public TypedExpression copy() {
    return new BooleanExpression(value);
  }

  @Override
  public String prettyString() {
    return "" + value;
  }
}
