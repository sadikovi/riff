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
 * [[LongExpression]] to hold long value that used for comparison for all typed bound
 * references.
 */
public class LongExpression implements TypedExpression<LongExpression> {
  // value is accessible to equality and comparison methods
  protected final long value;

  public LongExpression(long value) {
    this.value = value;
  }

  @Override
  public DataType dataType() {
    return DataTypes.LongType;
  }

  @Override
  public boolean eqExpr(InternalRow row, int ordinal) {
    return row.getLong(ordinal) == value;
  }

  @Override
  public boolean gtExpr(InternalRow row, int ordinal) {
    return row.getLong(ordinal) > value;
  }

  @Override
  public boolean ltExpr(InternalRow row, int ordinal) {
    return row.getLong(ordinal) < value;
  }

  @Override
  public boolean geExpr(InternalRow row, int ordinal) {
    return row.getLong(ordinal) >= value;
  }

  @Override
  public boolean leExpr(InternalRow row, int ordinal) {
    return row.getLong(ordinal) <= value;
  }

  @Override
  public boolean containsExpr(ColumnFilter filter) {
    return filter.mightContain(value);
  }

  @Override
  public int compareTo(LongExpression obj) {
    if (value == obj.value) return 0;
    return value < obj.value ? -1 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof LongExpression)) return false;
    LongExpression expr = (LongExpression) obj;
    return expr.value == value;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public TypedExpression copy() {
    return new LongExpression(value);
  }

  @Override
  public String prettyString() {
    return value + "L";
  }
}
