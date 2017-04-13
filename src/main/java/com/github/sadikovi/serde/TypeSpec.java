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
