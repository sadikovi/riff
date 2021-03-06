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

import java.io.Externalizable;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

/**
 * Internal schema specification based on Spark SQL schema, that acts as proxy to write and read
 * SQL rows. Note that type description columns index might be different from Spark SQL schema.
 */
public class TypeDescription implements Externalizable {
  private HashMap<String, TypeSpec> schema;
  // quick access to type spec by type description ordinal index
  private TypeSpec[] ordinalFields;
  // quick access arrays for specific field types
  private TypeSpec[] indexFields;
  private TypeSpec[] dataFields;

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
          throw new IllegalArgumentException("Found duplicate index column '" + name +
            "' in list " + Arrays.toString(indexColumns));
        }
        uniqueNames.add(name);
        TypeSpec spec = null;
        try {
          spec = new TypeSpec(schema.apply(name), true, numFields, schema.fieldIndex(name));
        } catch (IllegalArgumentException err) {
          // error message always includes trailing "."
          throw new IllegalArgumentException(err.getMessage() + " Schema " + schema, err);
        }
        this.indexFields[numFields] = spec;
        this.schema.put(spec.field().name(), spec);
        ++numFields;
      }
    } else {
      this.indexFields = new TypeSpec[0];
    }

    // resolve non-indexed fields
    this.dataFields = new TypeSpec[schema.length() - this.indexFields.length];
    int index = 0;
    for (StructField field : schema.fields()) {
      if (!this.schema.containsKey(field.name())) {
        TypeSpec spec = new TypeSpec(field, false, numFields, schema.fieldIndex(field.name()));
        // internal array index is different from numFields
        this.dataFields[index++] = spec;
        this.schema.put(field.name(), spec);
        ++numFields;
      }
    }

    // make sure that schema is consistently parsed and we do not have duplicated columns
    if (numFields != this.schema.size() || index != this.dataFields.length) {
      String msg = "";
      if (numFields != this.schema.size()) {
        msg = "total " + numFields + " != " + this.schema.size() + " in struct type";
      } else {
        msg = "non-indexed " + index + " != " + this.dataFields.length + " in struct type";
      }
      throw new RuntimeException(
        "Inconsistency of schema with type description (" + msg + "):\n" +
        "== Schema comparison ==\n" +
        "Type description: " + this + "\n" +
        "Schema: " + schema + "\n");
    }

    // initialize ordinal set of fields
    this.ordinalFields = new TypeSpec[numFields];
    for (int i = 0; i < this.indexFields.length; i++) {
      this.ordinalFields[this.indexFields[i].position()] = this.indexFields[i];
    }
    for (int i = 0; i < this.dataFields.length; i++) {
      this.ordinalFields[this.dataFields[i].position()] = this.dataFields[i];
    }
  }

  public TypeDescription(StructType schema) {
    this(schema, null);
  }

  // for serializer
  public TypeDescription() {
    this.schema = null;
    this.ordinalFields = null;
    this.indexFields = null;
    this.dataFields = null;
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
  public static boolean isSupportedDataType(DataType dataType) {
    return
      (dataType instanceof IntegerType) ||
      (dataType instanceof LongType) ||
      (dataType instanceof StringType) ||
      (dataType instanceof DateType) ||
      (dataType instanceof TimestampType) ||
      (dataType instanceof BooleanType) ||
      (dataType instanceof ShortType) ||
      (dataType instanceof ByteType);
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
  public TypeSpec[] dataFields() {
    return this.dataFields;
  }

  /**
   * Get an array of fields in ordinal positions. Internal array is returned and is assumed as
   * read-only. Position in array is equavalent of `position()` method for type spec.
   * @return array of TypeSpec instances
   */
  public TypeSpec[] fields() {
    return this.ordinalFields;
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
  public int position(String name) {
    TypeSpec spec = schema.get(name);
    if (spec == null) throw new NoSuchElementException("No such field " + name);
    return spec.position();
  }

  /**
   * Return type spec for ordinal in type description.
   * @param ordinal position in type description
   * @return TypeSpec instance
   */
  public TypeSpec atPosition(int ordinal) {
    return this.ordinalFields[ordinal];
  }

  /**
   * Convert this type description into Spark SQL struct type.
   * @return struct type
   */
  public StructType toStructType() {
    // StructField instances are immutable, therefore it is safe to pass them directly to
    // construct struct type
    StructType struct = new StructType();
    for (int i = 0; i < ordinalFields.length; i++) {
      struct = struct.add(ordinalFields[i].field());
    }
    return struct;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // type description ordinal fields array is only serialized, the rest can be reconstructed from
    // that array
    out.writeInt(ordinalFields.length);
    for (int i = 0; i < ordinalFields.length; i++) {
      out.writeObject(ordinalFields[i]);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt();
    this.ordinalFields = new TypeSpec[len];
    // number of index fields in this array
    int numIndexed = 0;
    for (int i = 0; i < len; i++) {
      this.ordinalFields[i] = (TypeSpec) in.readObject();
      if (this.ordinalFields[i].isIndexed()) {
        numIndexed++;
      }
    }
    // build index fields and data fields
    this.indexFields = new TypeSpec[numIndexed];
    this.dataFields = new TypeSpec[len - numIndexed];
    int k = 0, j = 0;
    for (int i = 0; i < len; i++) {
      if (this.ordinalFields[i].isIndexed()) {
        this.indexFields[k++] = this.ordinalFields[i];
      } else {
        this.dataFields[j++] = this.ordinalFields[i];
      }
    }
    // build hashmap for lookup
    this.schema = new HashMap<String, TypeSpec>(len);
    for (int i = 0; i < len; i++) {
      this.schema.put(this.ordinalFields[i].field().name(), this.ordinalFields[i]);
    }
  }

  /**
   * Write type description into external output stream.
   * Does not close stream.
   * @param out output stream
   * @throws IOException
   */
  public void writeTo(OutputStream out) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(out);
    oos.writeObject(this);
    oos.flush();
  }

  /**
   * Read type description from input stream.
   * Does not close stream.
   * @param stream input stream with object data
   * @throws IOException when io error happens, or class not found
   */
  public static TypeDescription readFrom(InputStream stream) throws IOException {
    ObjectInputStream in = new ObjectInputStream(stream);
    try {
      return (TypeDescription) in.readObject();
    } catch (ClassNotFoundException err) {
      throw new IOException("Failed to deserialize type description", err);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TypeDescription)) return false;
    TypeDescription td = (TypeDescription) obj;
    if (td == this) return true;
    if (td.size() != size()) return false;
    // positions in all fields array should be identical
    for (int i = 0; i < fields().length; i++) {
      if (!fields()[i].equals(td.fields()[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TypeDescription");
    sb.append(Arrays.toString(fields()));
    return sb.toString();
  }
}
