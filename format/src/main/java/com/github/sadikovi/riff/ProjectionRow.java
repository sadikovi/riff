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

package com.github.sadikovi.riff;

import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * [[ProjectionRow]] is a simple mutable row backed by array of objects with support for updates.
 * Needs to be in sync with supported types of [[IndexedRow]].
 */
public class ProjectionRow extends GenericInternalRow {
  // internal array to keep all values
  private final Object[] values;

  /**
   * Create instance of this row with expected number of fields.
   * @param size number of fields
   */
  public ProjectionRow(int size) {
    this.values = new Object[size];
  }

  /**
   * Create instance of this row with provided array.
   * Array reference is held by row, no copy is done.
   * @param values array of values
   */
  protected ProjectionRow(Object[] values) {
    this.values = values;
  }

  /**
   * Get underlying array of values. Used internally to compare and for testing.
   * @return array of values
   */
  protected Object[] values() {
    return values;
  }

  @Override
  public int numFields() {
    return values.length;
  }

  @Override
  public void update(int ordinal, Object value) {
    values[ordinal] = value;
  }

  @Override
  public ProjectionRow copy() {
    Object[] copy = new Object[values.length];
    System.arraycopy(values, 0, copy, 0, values.length);
    return new ProjectionRow(copy);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ProjectionRow)) return false;
    if (other == this) return true;
    ProjectionRow row = (ProjectionRow) other;
    return row.numFields() == this.numFields() && Arrays.equals(row.values(), this.values());
  }

  @Override
  public int hashCode() {
    int result = 31 * numFields();
    result += Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean anyNull() {
    int len = values.length, i = 0;
    while (i < len) {
      if (isNullAt(i)) return true;
      ++i;
    }
    return false;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return values[ordinal] == null;
  }

  /** Method to cast and retrieve integer value */
  private Integer getIntegerValue(int ordinal) {
    return (Integer) values[ordinal];
  }

  @Override
  public int getInt(int ordinal) {
    return getIntegerValue(ordinal).intValue();
  }

  /** Method to cast and retrieve long value */
  private Long getLongValue(int ordinal) {
    return (Long) values[ordinal];
  }

  @Override
  public long getLong(int ordinal) {
    return getLongValue(ordinal).longValue();
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return (UTF8String) values[ordinal];
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof IntegerType) {
      return getIntegerValue(ordinal);
    } else if (dataType instanceof LongType) {
      return getLongValue(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof DateType) {
      return getIntegerValue(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLongValue(ordinal);
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  @Override
  public String toString() {
    if (values.length == 0) {
      return "[empty row]";
    } else {
      return Arrays.toString(values);
    }
  }
}
