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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.ntree.TypedExpression;

/**
 * [[IntegerExpression]] to hold integer value that used for comparison for all typed bound
 * references.
 */
public class IntegerExpression implements TypedExpression<IntegerExpression> {
  // value is accessible to equality and comparison methods
  protected final int value;

  public IntegerExpression(int value) {
    this.value = value;
  }

  @Override
  public DataType dataType() {
    return DataTypes.IntegerType;
  }

  @Override
  public boolean eqExpr(InternalRow row, int ordinal) {
    return row.getInt(ordinal) == value;
  }

  @Override
  public boolean gtExpr(InternalRow row, int ordinal) {
    return row.getInt(ordinal) > value;
  }

  @Override
  public boolean ltExpr(InternalRow row, int ordinal) {
    return row.getInt(ordinal) < value;
  }

  @Override
  public boolean geExpr(InternalRow row, int ordinal) {
    return row.getInt(ordinal) >= value;
  }

  @Override
  public boolean leExpr(InternalRow row, int ordinal) {
    return row.getInt(ordinal) <= value;
  }

  @Override
  public boolean containsExpr(ColumnFilter filter) {
    return filter.mightContain(value);
  }

  @Override
  public int compareTo(IntegerExpression obj) {
    if (value == obj.value) return 0;
    return value < obj.value ? -1 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof IntegerExpression)) return false;
    IntegerExpression expr = (IntegerExpression) obj;
    return expr.value == value;
  }

  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public TypedExpression copy() {
    return new IntegerExpression(value);
  }

  @Override
  public String prettyString() {
    return "" + value;
  }
}
