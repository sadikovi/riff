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

import java.sql.Timestamp;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.github.sadikovi.riff.tree.TypedExpression;

/**
 * [[TimestampExpression]] to represent java.sql.Timestamp as long in Catalyst.
 */
public class TimestampExpression extends LongExpression {
  public TimestampExpression(Timestamp value) {
    super(DateTimeUtils.fromJavaTimestamp(value));
  }

  /** Internal constructor to convert directly to LongExpression */
  protected TimestampExpression(long value) {
    super(value);
  }

  @Override
  public DataType dataType() {
    return DataTypes.TimestampType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TimestampExpression)) return false;
    TimestampExpression expr = (TimestampExpression) obj;
    return expr.value == value;
  }

  @Override
  public TypedExpression copy() {
    return new TimestampExpression(value);
  }

  @Override
  public String prettyString() {
    return "TIMESTAMP(" + value + ")";
  }
}
