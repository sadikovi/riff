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

import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.Statistics;

/**
 * [[BoundReference]] represents leaf node with known name/ordinal and potentially value that this
 * name/ordinal binds to. This is a leaf node for filters, such as `EqualTo`, `GreaterThan`, etc.
 * Classes should inherit typed bound references if possible that provide typed value for each
 * implementation.
 *
 * TODO: Move all type information methods into TypedBoundReference, this will help "IsNull" to
 * remove any override methods that it does not support.
 * TODO: Introduce MultipleValuesBoundReference for filters, such as "In", this will remove boolean
 * flag `hasMultiplValues`.
 */
public abstract class BoundReference implements TreeNode {
  protected final String name;
  protected final int ordinal;

  public BoundReference(String name, int ordinal) {
    this.name = name;
    this.ordinal = ordinal;
  }

  /**
   * Get data type for value in bound reference.
   * This should return Spark SQL type associated wih value, and is used for resolving tree and
   * type check.
   * @return data type for filter value
   */
  public abstract DataType dataType();

  /**
   * Return a copy of this node with updated ordinal.
   * Method is used when resolving predicate tree.
   * @param newOrdinal ordinal of associated type spec
   * @return copy of current node with updated ordinal
   */
  public abstract BoundReference withOrdinal(int newOrdinal);

  /**
   * Tree operator as string, for example. equality would return "=".
   * @return operator symbol
   */
  public abstract String treeOperator();

  /**
   * Get name for reference.
   * @return name
   */
  public String name() {
    return name;
  }

  /**
   * Get ordinal for reference. Note that for unresolved node ordinal may not be valid.
   * @return ordinal
   */
  public int ordinal() {
    return ordinal;
  }

  @Override
  public final boolean resolved() {
    // return true if ordinal is a valid array index
    return ordinal >= 0;
  }

  /**
   * Whether or not node has multiple values to compare.
   * This is done mainly for "In" predicate, which is backed an array.
   * @return true if node has many values, false otherwise
   */
  public abstract boolean hasMultiplValues();

  //////////////////////////////////////////////////////////////
  // Typed methods to extract values.
  // Appropriate one should always be overwritten based on data type
  //////////////////////////////////////////////////////////////

  int getInt() { throw new UnsupportedOperationException(); }

  long getLong() { throw new UnsupportedOperationException(); }

  UTF8String getUTF8String() { throw new UnsupportedOperationException(); }

  int[] getIntArray() { throw new UnsupportedOperationException(); }

  long[] getLongArray() { throw new UnsupportedOperationException(); }

  UTF8String[] getUTF8StringArray() { throw new UnsupportedOperationException(); }

  //////////////////////////////////////////////////////////////
  // Standard equals, hashCode and toString typed methods
  //////////////////////////////////////////////////////////////

  @Override
  public boolean equals(Object obj) {
    // equals method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    if (obj == null || obj.getClass() != this.getClass()) return false;
    BoundReference that = (BoundReference) obj;
    if (!name.equals(that.name) || ordinal != that.ordinal || !dataType().equals(that.dataType())) {
      return false;
    }
    if (dataType() instanceof IntegerType) {
      if (hasMultiplValues()) {
        return Arrays.equals(getIntArray(), that.getIntArray());
      } else {
        return getInt() == that.getInt();
      }
    } else if (dataType() instanceof LongType) {
      if (hasMultiplValues()) {
        return Arrays.equals(getLongArray(), that.getLongArray());
      } else {
        return getLong() == that.getLong();
      }
    } else if (dataType() instanceof StringType) {
      if (hasMultiplValues()) {
        return Arrays.equals(getUTF8StringArray(), that.getUTF8StringArray());
      } else {
        return getUTF8String().equals(that.getUTF8String());
      }
    } else if (dataType() instanceof NullType) {
      // this should only work for "IsNull"
      return true;
    } else {
      throw new UnsupportedOperationException("Type " + dataType());
    }
  }

  @Override
  public int hashCode() {
    // hashCode method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    // value hash code, does not affect hashCode by default
    int valueCode = 0;
    if (dataType() instanceof IntegerType) {
      valueCode = (hasMultiplValues()) ? Arrays.hashCode(getIntArray()) : getInt();
    } else if (dataType() instanceof LongType) {
      if (hasMultiplValues()) {
        valueCode = Arrays.hashCode(getLongArray());
      } else {
        valueCode = (int) (getLong() ^ (getLong() >>> 32));
      }
    } else if (dataType() instanceof StringType) {
      if (hasMultiplValues()) {
        valueCode = Arrays.hashCode(getUTF8StringArray());
      } else {
        valueCode = getUTF8String().hashCode();
      }
    } else if (dataType() instanceof NullType) {
      // this should only work for "IsNull"
      valueCode = 0;
    } else {
      throw new UnsupportedOperationException("Type " + dataType());
    }
    int result = 31 * ordinal + name.hashCode();
    result = result * 31 + valueCode;
    return 31 * result + getClass().hashCode();
  }

  /**
   * Return pretty string representation for value.
   * @return string representation for value
   */
  public String prettyValue() {
    if (dataType() instanceof IntegerType) {
      if (hasMultiplValues()) {
        return Arrays.toString(getIntArray());
      } else {
        return Integer.toString(getInt());
      }
    } else if (dataType() instanceof LongType) {
      if (hasMultiplValues()) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < getLongArray().length; i++) {
          sb.append(getLongArray()[i] + "L");
          if (i < getLongArray().length - 1) {
            sb.append(", ");
          }
        }
        sb.append("]");
        return sb.toString();
      } else {
        return Long.toString(getLong()) + "L";
      }
    } else if (dataType() instanceof StringType) {
      if (hasMultiplValues()) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < getUTF8StringArray().length; i++) {
          sb.append("'");
          sb.append(getUTF8StringArray()[i]);
          sb.append("'");
          if (i < getUTF8StringArray().length - 1) {
            sb.append(", ");
          }
        }
        sb.append("]");
        return sb.toString();
      } else {
        return "'" + getUTF8String() + "'";
      }
    } else if (dataType() instanceof NullType) {
      return "null";
    } else {
      throw new UnsupportedOperationException("Type " + dataType());
    }
  }

  @Override
  public final String toString() {
    String tag = resolved() ? (name + "[" + ordinal + "]") : ("*" + name);
    return tag + " " + treeOperator() + " " + prettyValue();
  }

  //////////////////////////////////////////////////////////////
  // Statistics support
  // Overwrite any of the methods below if node can be evaluated based on statistics
  //////////////////////////////////////////////////////////////

  @Override
  public final boolean evaluate(Statistics[] stats) {
    // we do not enforce tree being resolved, instead the behaviour is undefined.
    // to enforce correct result, resolve tree first and check with `resolved()` method.
    return stats[ordinal].evaluateState(this);
  }

  /**
   * Update node with statistics information about null values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param hasNulls true if statistics has nulls, false otherwise
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(boolean hasNulls) {
    return true;
  }

  /**
   * Update node with integer statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min int value
   * @param max max int value
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(int min, int max) {
    return true;
  }

  /**
   * Update node with long statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min long value
   * @param max max long value
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(long min, long max) {
    return true;
  }

  /**
   * Update node with UTF8String statistics values.
   * Should not modify statistics values.
   * If information is not used return true, otherwise return status based on update.
   * @param min min UTF8 value or null
   * @param max max UTF8 value or null
   * @return true if predicate passes statistics, false otherwise
   */
  public boolean statUpdate(UTF8String min, UTF8String max) {
    return true;
  }

  //////////////////////////////////////////////////////////////
  // Column filter support
  // Overwrite any of the methods below if node can be evaluated based on column filters
  //////////////////////////////////////////////////////////////

  @Override
  public boolean evaluate(ColumnFilter[] filters) {
    // method assumes that tree is resolved to access ordinal
    return evaluateFilter(filters[ordinal]);
  }

  /**
   * Evaluate column filter for this bound reference.
   * This should invoke `mightContain` method of filter if this node supports filtering.
   * @param filter for this node
   * @return true if node value passes filter or unknown, false if value does not pass filter
   */
  protected boolean evaluateFilter(ColumnFilter filter) {
    return true;
  }
}
