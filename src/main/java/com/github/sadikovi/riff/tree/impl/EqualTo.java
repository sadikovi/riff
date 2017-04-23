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

package com.github.sadikovi.riff.tree.impl;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.Statistics;
import com.github.sadikovi.riff.tree.LeafNode;
import com.github.sadikovi.riff.tree.Modifier;
import com.github.sadikovi.riff.tree.TreeNode;

public abstract class EqualTo extends LeafNode {
  public EqualTo(String name, int ordinal) {
    super(name, ordinal);
  }

  @Override
  public TreeNode transform(Modifier modifier) {
    return modifier.update(this);
  }

  static class EqualToInt extends EqualTo {
    private final int value;

    public EqualToInt(String name, int ordinal, int value) {
      super(name, ordinal);
      this.value = value;
    }

    public int value() {
      return this.value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getInt(ordinal) == value;
    }

    @Override
    public boolean statUpdate(int min, int max) {
      return min >= value && value <= max;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof EqualToInt)) return false;
      EqualToInt that = (EqualToInt) obj;
      return name().equals(that.name()) && ordinal() == that.ordinal() && value() == that.value();
    }

    @Override
    public int hashCode() {
      int result = 31 * ordinal + name.hashCode();
      result = result * 31 + value;
      return 31 * result + getClass().hashCode();
    }

    @Override
    public String toString(String tag) {
      return tag + " = " + value;
    }
  }

  static class EqualToLong extends EqualTo {
    private final long value;

    public EqualToLong(String name, int ordinal, int value) {
      super(name, ordinal);
      this.value = value;
    }

    public long value() {
      return this.value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getLong(ordinal) == value;
    }

    @Override
    public boolean statUpdate(long min, long max) {
      return min >= value && value <= max;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof EqualToLong)) return false;
      EqualToLong that = (EqualToLong) obj;
      return name().equals(that.name()) && ordinal() == that.ordinal() && value() == that.value();
    }

    @Override
    public int hashCode() {
      int result = 31 * ordinal + name.hashCode();
      result = result * 31 + (int) (value ^ (value >>> 32));
      return 31 * result + getClass().hashCode();
    }

    @Override
    public String toString(String tag) {
      return tag + " = " + value + "L";
    }
  }

  static class EqualToUTF8 extends EqualTo {
    private final UTF8String value;

    public EqualToUTF8(String name, int ordinal, UTF8String value) {
      super(name, ordinal);
      this.value = value;
    }

    public UTF8String value() {
      return this.value;
    }

    @Override
    protected boolean evaluate(InternalRow row, int ordinal) {
      return row.getUTF8String(ordinal).equals(value);
    }

    @Override
    public boolean statUpdate(UTF8String min, UTF8String max) {
      if (min == null && max == null) return false;
      return value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof EqualToUTF8)) return false;
      EqualToUTF8 that = (EqualToUTF8) obj;
      return name().equals(that.name()) && ordinal() == that.ordinal() &&
        value().equals(that.value());
    }

    @Override
    public int hashCode() {
      int result = 31 * ordinal + name.hashCode();
      result = result * 31 + value.hashCode();
      return 31 * result + getClass().hashCode();
    }

    @Override
    public String toString(String tag) {
      return tag + " = '" + value + "'";
    }
  }
}
