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
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.ntree.TypedExpression;

/**
 * [[UTF8StringExpression]] to hold UTF8String value that used for comparison for all typed bound
 * references.
 */
public class UTF8StringExpression implements TypedExpression<UTF8StringExpression> {
  // value is accessible to equality and comparison methods
  protected final UTF8String value;

  public UTF8StringExpression(UTF8String value) {
    this.value = value;
  }

  @Override
  public DataType dataType() {
    return DataTypes.StringType;
  }

  @Override
  public boolean eqExpr(InternalRow row, int ordinal) {
    // equality for UTF8String is faster and based on direct byte array comparison
    return row.getUTF8String(ordinal).equals(value);
  }

  @Override
  public boolean gtExpr(InternalRow row, int ordinal) {
    return row.getUTF8String(ordinal).compareTo(value) > 0;
  }

  @Override
  public boolean ltExpr(InternalRow row, int ordinal) {
    return row.getUTF8String(ordinal).compareTo(value) < 0;
  }

  @Override
  public boolean geExpr(InternalRow row, int ordinal) {
    return row.getUTF8String(ordinal).compareTo(value) >= 0;
  }

  @Override
  public boolean leExpr(InternalRow row, int ordinal) {
    return row.getUTF8String(ordinal).compareTo(value) <= 0;
  }

  @Override
  public boolean containsExpr(ColumnFilter filter) {
    return filter.mightContain(value);
  }

  @Override
  public int compareTo(UTF8StringExpression obj) {
    // compareTo method for UTF8String returns either positive, 0, or negative
    // non-zero values are not necessarily 1 and -1
    return value.compareTo(obj.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof UTF8StringExpression)) return false;
    UTF8StringExpression expr = (UTF8StringExpression) obj;
    return expr.value.equals(value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public TypedExpression copy() {
    return new UTF8StringExpression(UTF8String.fromBytes(value.getBytes()));
  }

  @Override
  public String prettyString() {
    return "'" + value + "'";
  }
}
