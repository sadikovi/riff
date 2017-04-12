package com.github.sadikovi.serde;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Internal schema specification based on Spark SQL schema, that acts as proxy to write and read
 * SQL rows. Note that type description columns index might be different from Spark SQL schema.
 */
public class TypeDescription {
  private final StructType schema;
  private final StructField[] indexFields;
  private final StructField[] nonIndexFields;

  public TypeDescription(StructType schema, String[] indexColumns) {
    assertSchema(schema);
    this.schema = schema;
    // resolve indexed fields first
    if (indexColumns != null && indexColumns.length > 0) {
      HashSet<String> uniqueNames = new HashSet<String>();
      ArrayList<StructField> temp = new ArrayList<StructField>();
      for (String name : indexColumns) {
        if (uniqueNames.contains(name)) {
          throw new IllegalArgumentException("Found duplicate index column " + name);
        }
        uniqueNames.add(name);
        temp.add(this.schema.apply(name));
      }
      this.indexFields = new StructField[temp.size()];
      for (int i = 0; i < temp.size(); i++) {
        this.indexFields[i] = temp.get(i);
      }
    } else {
      this.indexFields = new StructField[0];
    }

    // resolve non-indexed fields
    this.nonIndexFields = new StructField[this.schema.length() - this.indexFields.length];
    int i = 0;
    for (StructField field : this.schema.fields()) {
      if (!isIndexed(field)) {
        this.nonIndexFields[i++] = field;
      }
    }
  }

  public TypeDescription(StructType schema) {
    this(schema, null);
  }

  /** Check if schema is valid, contains supported types */
  private void assertSchema(StructType schema) {
    if (schema == null || schema.fields().length < 1) {
      throw new UnsupportedOperationException(
        "Schema has insufficient number of columns, " + schema + ", expected at least one column");
    }

    for (StructField field : schema.fields()) {
      if (!isSupportedDataType(field.dataType())) {
        throw new UnsupportedOperationException("Field " + field + " is not supported");
      }
    }
  }

  /** Whether or not field with name and data type is supported */
  public boolean isSupportedDataType(DataType dataType) {
    return
      (dataType instanceof IntegerType) ||
      (dataType instanceof LongType) ||
      (dataType instanceof StringType);
  }

  /** Do reference comparison to check if current field is already registered as indexed field */
  private boolean isIndexed(StructField field) {
    for (StructField indexedField : this.indexFields) {
      if (indexedField == field) return true;
    }
    return false;
  }

  /**
   * Return only indexed fields; if no indexed fields provided, empty array is returned.
   */
  public StructField[] indexFields() {
    return this.indexFields;
  }

  /**
   * Return non-indexed fields; if no such fields exist, empty array is returned.
   */
  public StructField[] nonIndexFields() {
    return this.nonIndexFields;
  }

  /**
   * Return all fields in schema.
   */
  public StructField[] fields() {
    return this.schema.fields();
  }

  /**
   * Number of fields in schema (type description).
   */
  public int size() {
    return this.schema.length();
  }

  /**
   * Return type description index for field name.
   */
  public int index(String name) {
    int i = 0;
    // check indexed columns
    while (i < this.indexFields.length) {
      if (this.indexFields[i].name().equals(name)) {
        return i;
      }
      ++i;
    }
    // check non-indexed columns
    while (i < this.nonIndexFields.length) {
      if (this.nonIndexFields[i - this.indexFields.length].name().equals(name)) {
        return i;
      }
      ++i;
    }
    throw new IllegalArgumentException("Invalid field name " + name + ", desc " + this);
  }

  /**
   * Index of the field in underlying struct type, that was provided to construct this type
   * description.
   */
  public int structTypeIndex(String name) {
    return this.schema.fieldIndex(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < this.schema.fields().length; i++) {
      StructField field = this.schema.fields()[i];
      sb.append(field.name() + ": " + field.dataType().simpleString());
      if (i < this.schema.fields().length - 1) {
        sb.append(", ");
      }
    }
    return "TypeDescription[" + sb.toString() + "]";
  }
}
