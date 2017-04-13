package com.github.sadikovi.serde;

import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;

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
  public static class TypeSpec {
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

  private HashMap<String, TypeSpec> schema;
  // quick access arrays for specific field types
  private TypeSpec[] indexFields;
  private TypeSpec[] nonIndexFields;

  public TypeDescription(StructType schema, String[] indexColumns) {
    assertSchema(schema);
    this.schema = new HashMap<String, TypeSpec>();
    int numFields = 0;
    // resolve indexed fields first, indexed fields are checked on uniqueness
    if (indexColumns != null && indexColumns.length > 0) {
      this.indexFields = new TypeSpec[indexColumns.length];
      HashSet<String> uniqueNames = new HashSet<String>();
      for (String name : indexColumns) {
        if (uniqueNames.contains(name)) {
          throw new IllegalArgumentException("Found duplicate index column " + name);
        }
        uniqueNames.add(name);
        TypeSpec spec = new TypeSpec(schema.apply(name), true, numFields, schema.fieldIndex(name));
        this.indexFields[numFields] = spec;
        this.schema.put(spec.field().name(), spec);
        ++numFields;
      }
    } else {
      this.indexFields = new TypeSpec[0];
    }

    // resolve non-indexed fields
    this.nonIndexFields = new TypeSpec[schema.length() - this.indexFields.length];
    int index = 0;
    for (StructField field : schema.fields()) {
      if (!this.schema.containsKey(field.name())) {
        TypeSpec spec = new TypeSpec(field, false, numFields, schema.fieldIndex(field.name()));
        // internal array index is different from numFields
        this.nonIndexFields[index++] = spec;
        this.schema.put(field.name(), spec);
        ++numFields;
      }
    }

    // make sure that schema is consistently parsed and we do not have duplicated columns
    if (numFields != this.schema.size() || index != this.nonIndexFields.length) {
      String msg = "";
      if (numFields != this.schema.size()) {
        msg = "total " + numFields + " != " + this.schema.size() + " in struct type";
      } else {
        msg = "non-indexed " + index + " != " + this.nonIndexFields.length + " in struct type";
      }
      throw new RuntimeException(
        "Inconsistency of schema with type description (" + msg + "):\n" +
        "== Schema comparison ==\n" +
        "Type description: " + this + "\n" +
        "Schema: " + schema + "\n");
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
        throw new UnsupportedOperationException("Field " + field + " with type " +
          field.dataType() + " is not supported");
      }
    }
  }

  /**
   * Whether or not field with name and data type is supported
   * @return true if type is supported, false otherwise
   */
  public boolean isSupportedDataType(DataType dataType) {
    return
      (dataType instanceof IntegerType) ||
      (dataType instanceof LongType) ||
      (dataType instanceof StringType);
  }

  /**
   * Whether or not field for provided name is indexed.
   * @param name field name
   * @return true if field is indexed, false otherwise
   */
  private boolean isIndexed(String name) {
    TypeSpec spec = this.schema.get(name);
    if (spec == null) throw new NoSuchElementException("No such field " + name);
    return spec.isIndexed();
  }

  /**
   * Return only indexed fields; if no indexed fields provided, empty array is returned. It is safe
   * to use array index as position - fields are sorted by their `position()` in type description.
   * @return array of indexed TypeSpec fields
   */
  public TypeSpec[] indexFields() {
    return this.indexFields;
  }

  /**
   * Return non-indexed fields; if no such fields exist, empty array is returned. It is safe to use
   * array index as position - fields are sorted by their `position()` in type description.
   * @return array of non-indexed TypeSpec fields
   */
  public TypeSpec[] nonIndexFields() {
    return this.nonIndexFields;
  }

  /**
   * Number of fields in type description
   * @return size of internal map
   */
  public int size() {
    return this.schema.size();
  }

  /**
   * Return type description index for field name.
   * @param name field name
   * @return index in type description
   */
  public int index(String name) {
    TypeSpec spec = schema.get(name);
    if (spec == null) throw new NoSuchElementException("No such field " + name);
    return spec.position();
  }

  @Override
  public String toString() {
    return "TypeDescription(" + this.schema + ")";
  }
}
