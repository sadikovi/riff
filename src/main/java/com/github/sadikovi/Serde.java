package com.github.sadikovi;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Row is serialized similar to Spark SQL, meaning we have null bits region, fixed length values
 * region and variable length values region. We also write keys that are not part of the general
 * layout. Reason is having fast access to the keys, without reconstructing record - this will
 * allow to check in serialized form.
 */
public class Serde {
  private final StructType schema;
  private final String[] keyColumns;
  // buffer to store data for key columns
  private DataOutputStream keyBuffer;
  // buffer to write fixed portion of row
  private DataOutputStream fixBuffer;
  // buffer to write variable portion of row
  private DataOutputStream varBuffer;

  public Serde(StructType schema, String[] keys) {
    // make sure that schema is supported
    assertSchema(schema);
    this.schema = schema;
    // make sure that key columns are part of the schema
    assertKeyColumns(this.schema, keys);
    this.keyColumns = keys;
  }

  /**
   * Write provided row into output buffer.
   * @param row row to write, must confirm to the schema
   * @param out output buffer to write
   */
  public void writeRow(Row row, ByteBuffer out) throws IOException {
    // reset current buffers
    prepareWrite();
    writeKeyColumns(row);
    writeValues(row);
    flush(out);
  }

  private void prepareWrite() {
    // reset temporary buffers and state
    // use buffers as simple byte streams, since we need to grow size of underlying byte array
    keyBuffer = new DataOutputStream(new ByteArrayOutputStream());
    fixBuffer = new DataOutputStream(new ByteArrayOutputStream());
    varBuffer = new DataOutputStream(new ByteArrayOutputStream());
  }

  /**
   * Key columns are written directly, in the format:
   * +--------------+--------------------------+
   * | is_null byte | fixed length value bytes |
   * +--------------+--------------------------+
   * For variable length fields we write:
   * +--------------+----------------+-------------------+
   * | is_null byte | length 4 bytes | sequence of bytes |
   * +--------------+----------------+-------------------+
   * If null bit is set, no following bytes are expected
   */
  private void writeKeyColumns(Row row) throws IOException {
    // extract key columns and write them into temporary buffer
    int i = 0;
    StructField field = null;
    while (i < this.keyColumns.length) {
      int index = schema.fieldIndex(this.keyColumns[i]);
      this.keyBuffer.writeBoolean(row.isNullAt(index));
      if (!row.isNullAt(index)) {
        field = schema.apply(index);
        writeDirect(row.get(index), field.dataType(), this.keyBuffer);
      }
      ++i;
    }
  }

  /** Write direct value, value is guaranteed to be non-null */
  private void writeDirect(Object value, DataType dataType, DataOutputStream out) {

  }

  private void writeValues(Row row) {
    // write the rest of the values
  }

  private void flush(ByteBuffer out) {
    // write all buffers into output buffer, release resources.
  }

  /** Return true, if type is supported, currently only support non-complex types */
  private boolean isTypeSupported(DataType tpe) {
    return
      (tpe instanceof IntegerType) ||
      (tpe instanceof LongType) ||
      (tpe instanceof ShortType) ||
      (tpe instanceof ByteType) ||
      (tpe instanceof StringType);
  }

  /**
   * Make sure that schema is valid, with at least one column, and has supported types only.
   * Otherwise, throw an error, describing the difference.
   */
  private void assertSchema(StructType schema) {
    if (schema == null || schema.fields().length < 1) {
      throw new UnsupportedOperationException(
        "Schema has insufficient number of columns, " + schema);
    }

    for (StructField field : schema.fields()) {
      if (!isTypeSupported(field.dataType())) {
        throw new UnsupportedOperationException("Unsupported type " + field.dataType() +
          " in schema " + schema);
      }
    }
  }

  /**
   * Validate key columns against the provided schema; columns must exist, be non-null and
   * non-empty, and also should not contain duplicates. Schema is assumed to be correct.
   * Columns array can be empty, meaning no key columns provided to keep.
   */
  private void assertKeyColumns(StructType schema, String[] columns) {
    HashSet<String> set = new HashSet<String>();
    for (String column : columns) {
      if (column == null || column.length() == 0) {
        throw new UnsupportedOperationException("Invalid key column name " + column +
          " in array " + Arrays.toString(columns));
      }
      if (set.contains(column)) {
        throw new UnsupportedOperationException("Duplicate column name is detected, " + column);
      }
      set.add(column);
    }

    for (StructField field : schema.fields()) {
      if (set.contains(field.name())) {
        set.remove(field.name());
      }
    }

    if (!set.isEmpty()) {
      throw new UnsupportedOperationException("Could not resolve fields " + set +
        ", they do not exist in schema " + schema);
    }
  }
}
