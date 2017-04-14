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

package com.github.sadikovi.serde;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

public class TypeSpec {
  // SQL field specification
  private StructField field;
  // whether or not this field should be used for indexing
  private boolean indexed;
  // position for type description
  private int pos;
  // original position of the StructField in SQL schema, used for writes
  private int origPos;

  TypeSpec(StructField field, boolean indexed, int pos, int origPos) {
    this.field = field;
    this.indexed = indexed;
    this.pos = pos;
    this.origPos = origPos;
  }

  /** Get field */
  public StructField field() {
    return this.field;
  }

  /** Get field data type */
  public DataType dataType() {
    return this.field.dataType();
  }

  /** Whether or not current field is indexed */
  public boolean isIndexed() {
    return this.indexed;
  }

  /** Get field position */
  public int position() {
    return this.pos;
  }

  /** Get original position for StructField, used for writes only */
  public int origSQLPos() {
    return this.origPos;
  }

  @Override
  public String toString() {
    return "TypeSpec(" + this.field.name() + ": " + this.field.dataType().simpleString() +
      ", indexed=" + this.indexed + ", position=" + this.pos + ", origPos=" + this.origPos + ")";
  }
}
