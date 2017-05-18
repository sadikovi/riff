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

import java.sql.Date;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.github.sadikovi.riff.tree.TypedExpression;

/**
 * [[DateExpression]] to represent java.sql.Date as integer in Catalyst.
 */
public class DateExpression extends IntegerExpression {
  public DateExpression(Date value) {
    super(DateTimeUtils.fromJavaDate(value));
  }

  /** Internal constructor to convert directly to IntegerExpression */
  protected DateExpression(int value) {
    super(value);
  }

  @Override
  public DataType dataType() {
    return DataTypes.DateType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof DateExpression)) return false;
    DateExpression expr = (DateExpression) obj;
    return expr.value == value;
  }

  @Override
  public TypedExpression copy() {
    return new DateExpression(value);
  }

  @Override
  public String prettyString() {
    return "DATE(" + value + ")";
  }
}
