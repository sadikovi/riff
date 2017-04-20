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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * [[GenericInternalRow]] is a placeholder with default implementations for SQL internal row. Also
 * adds specific to the mutable row methods for writing/reading data from stream. It is recommended
 * that subclasses provide only methods they support without overwriting all methods of the generic
 * row.
 */
abstract class GenericInternalRow extends InternalRow {
  GenericInternalRow() { }

  @Override
  public int numFields() {
    throw new UnsupportedOperationException();
  }

  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public final void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  public void setBoolean(int ordinal, boolean value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(int ordinal, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(int ordinal, short value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(int ordinal, int value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(int ordinal, long value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(int ordinal, float value) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(int ordinal, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InternalRow copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  //////////////////////////////////////////////////////////////
  // Specialized getters
  //////////////////////////////////////////////////////////////

  @Override
  public boolean isNullAt(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayData getArray(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    throw new UnsupportedOperationException();
  }
}
