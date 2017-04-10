package com.github.sadikovi.serde;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.spark.sql.catalyst.InternalRow;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.unsafe.types.UTF8String;

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
  private ObjectOutputStream keyBuffer;
  // buffer to write fixed portion of row
  private ObjectOutputStream fixBuffer;
  // buffer to write variable portion of row
  private ObjectOutputStream varBuffer;
  // data type writers
  private RowValueWriter[] writers;

  public Serde(StructType schema, String[] keys) {
    // make sure that schema is supported
    assertSchema(schema);
    this.schema = schema;
    this.writers = initializeWriters(this.schema);
    // make sure that key columns are part of the schema
    assertKeyColumns(this.schema, keys);
    this.keyColumns = keys;
  }

  /**
   * Write provided row into output buffer.
   * @param row row to write, must confirm to the schema
   * @param out output buffer to write
   */
  public void writeRow(InternalRow row, ByteBuffer out) throws IOException {
    // reset current buffers
    prepareWrite();
    writeKeyColumns(row);
    writeValues(row);
    flush(out);
  }

  private void prepareWrite() throws IOException {
    // reset temporary buffers and state
    // use buffers as simple byte streams, since we need to grow size of underlying byte array
    keyBuffer = new ObjectOutputStream(new ByteArrayOutputStream());
    fixBuffer = new ObjectOutputStream(new ByteArrayOutputStream());
    varBuffer = new ObjectOutputStream(new ByteArrayOutputStream());
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
  private void writeKeyColumns(InternalRow row) throws IOException {
    // extract key columns and write them into temporary buffer
    int i = 0;
    while (i < this.keyColumns.length) {
      int index = schema.fieldIndex(this.keyColumns[i]);
      this.keyBuffer.writeBoolean(row.isNullAt(index));
      if (!row.isNullAt(index)) {
        this.writers[index].write(row, index, this.keyBuffer);
      }
      ++i;
    }
  }

  private void writeValues(InternalRow row) {
    // write the rest of the values
  }

  private void flush(ByteBuffer out) {
    // write all buffers into output buffer, release resources.
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

  /** Initialize writers for each supported field type */
  private RowValueWriter[] initializeWriters(StructType schema) {
    RowValueWriter[] writers = new RowValueWriter[schema.fields().length];
    int i = 0;
    while (i < schema.fields().length) {
      DataType dataType = schema.apply(i).dataType();
      if (dataType instanceof IntegerType) {
        writers[i] = new RowInteger();
      } else if (dataType instanceof LongType) {
        writers[i] = new RowLong();
      } else if (dataType instanceof ShortType) {
        writers[i] = new RowShort();
      } else if (dataType instanceof ByteType) {
        writers[i] = new RowByte();
      } else if (dataType instanceof BooleanType) {
        writers[i] = new RowBoolean();
      } else if (dataType instanceof StringType) {
        writers[i] = new RowString();
      } else {
        throw new UnsupportedOperationException("Type " + dataType);
      }
    }
    return writers;
  }

  //////////////////////////////////////////////////////////////
  // == Row writers and readers ==
  //////////////////////////////////////////////////////////////

  static class RowInteger implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeInt(row.getInt(ordinal));
    }
  }

  static class RowLong implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeLong(row.getLong(ordinal));
    }
  }

  static class RowShort implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeShort(row.getShort(ordinal));
    }
  }

  static class RowByte implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeByte(row.getByte(ordinal));
    }
  }

  static class RowBoolean implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeBoolean(row.getBoolean(ordinal));
    }
  }

  static class RowString implements RowValueWriter {
    @Override
    public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException {
      UTF8String value = row.getUTF8String(ordinal);
      // string is written as (bytes length) -> (bytes sequence)
      value.writeExternal(buffer);
    }
  }
}
