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

package com.github.sadikovi.riff.ntree;

import org.apache.spark.sql.types.DataType;

import com.github.sadikovi.riff.TypeSpec;

/**
 * Bound reference that has type information and value expression.
 */
public abstract class TypedBoundReference extends BoundReference {

  /**
   * Typed expression for this reference.
   * @return typed expression
   */
  public abstract TypedExpression expression();

  /**
   * Operator symbol for typed bound reference.
   * @return operator
   */
  public abstract String operator();

  /**
   * Data type associated with this typed bound reference
   * @return Spark SQL data type
   */
  public DataType dataType() {
    return expression().dataType();
  }

  @Override
  public void update(TypeSpec spec) {
    if (!spec.dataType().equals(dataType())) {
      throw new IllegalStateException("Type mismatch: " + spec.dataType() + " != " + dataType() +
      ", spec=" + spec + ", tree={" + this + "}");
    }
  }

  @Override
  public boolean equals(Object obj) {
    // equals method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    if (obj == null || obj.getClass() != this.getClass()) return false;
    TypedBoundReference that = (TypedBoundReference) obj;
    return name().equals(that.name()) && ordinal() == that.ordinal() &&
      expression().equals(that.expression());
  }

  @Override
  public int hashCode() {
    // hashCode method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    int result = 31 * ordinal() + name().hashCode();
    result = 31 * result + expression().hashCode();
    return 31 * result + getClass().hashCode();
  }

  @Override
  public String toString() {
    return prettyName() + " " + operator() + " " + expression().prettyString();
  }
}
