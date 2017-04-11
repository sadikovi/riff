package com.github.sadikovi.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.spark.sql.catalyst.InternalRow;

import org.apache.spark.sql.types.StructType;

/**
 * Row is serialized similar to Spark SQL, meaning we have null bits region, fixed length values
 * region and variable length values region. We also write keys that are not part of the general
 * layout. Reason is having fast access to the keys, without reconstructing record - this will
 * allow to check in serialized form.
 * Not thread-safe.
 */
public class RowWriter {
  private final TypeDescription desc;
  // buffer to store data for indexed columns
  private ByteArrayOutputStream indexedBuffer;
  // buffer to write fixed portion of row
  private ByteArrayOutputStream fixedBuffer;
  // buffer to write variable portion of row
  private ByteArrayOutputStream variableBuffer;
  // object output stream handlers
  private ObjectOutputStream indexedBufHandler;
  private ObjectOutputStream fixedBufHandler;
  private ObjectOutputStream variableBufHandler;
  // set of converters to use
  private final RowValueConverter[] converters;

  public RowWriter(StructType schema, String[] indexedColumns) {
    this.desc = new TypeDescription(schema, indexedColumns);
    // these byte buffers are reset and reused between serializations
    this.indexedBuffer = new ByteArrayOutputStream();
    this.fixedBuffer = new ByteArrayOutputStream();
    this.variableBuffer = new ByteArrayOutputStream();
    // these object output streams are reinitialized for every serialization
    this.indexedBufHandler = null;
    this.fixedBufHandler = null;
    this.variableBufHandler = null;
    // initialize converters, they are reused across rows
    this.converters = new RowValueConverter[this.desc.size()];
    for (int i = 0; i < this.desc.size(); i++) {
      this.converters[i] = Converters.sqlTypeToConverter(this.desc.fields()[i].dataType());
    }
  }

  /**
   * Write provided row into output buffer.
   * @param row row to write, must confirm to the schema
   * @param out output buffer to write
   */
  public void writeRow(InternalRow row, OutputStream out) throws IOException {
    // reset current buffers
    prepareWrite();
    writeIndexedFields(row);
    writeFields(row);
    flush(out);
  }

  private void prepareWrite() throws IOException {
    // reset temporary buffers and state
    this.indexedBuffer.reset();
    this.fixedBuffer.reset();
    this.variableBuffer.reset();
    // use buffers as simple byte streams, since we need to grow size of underlying byte array
    this.indexedBufHandler = new ObjectOutputStream(this.indexedBuffer);
    this.fixedBufHandler = new ObjectOutputStream(this.fixedBuffer);
    this.variableBufHandler = new ObjectOutputStream(this.variableBuffer);
  }

  /**
   * Indexed fields are written directly, in the format:
   * +--------------+--------------------------+
   * | is_null byte | fixed length value bytes |
   * +--------------+--------------------------+
   * For variable length fields we write:
   * +--------------+----------------+-------------------+
   * | is_null byte | length 4 bytes | sequence of bytes |
   * +--------------+----------------+-------------------+
   * If null bit is set, no following bytes are expected
   */
  private void writeIndexedFields(InternalRow row) throws IOException {
    // extract indexed fields and write them into temporary buffer
    int i = 0;
    while (i < this.desc.indexFields().length) {
      int ordinal = this.desc.fieldIndex(this.desc.indexFields()[i].name());
      this.indexedBufHandler.writeBoolean(row.isNullAt(ordinal));
      if (!row.isNullAt(ordinal)) {
        this.converters[ordinal].write(row, ordinal, this.indexedBufHandler);
      }
      ++i;
    }
  }

  /**
   * Write non-indexed fields; this is similar to Spark SQL representation:
   * +-----------+---------------------+------------------------+
   * | null bits | fixed length values | variable length values |
   * +-----------+---------------------+------------------------+
   * Values are written back-to-back.
   * If null bit is set, nothing is written at field position.
   */
  private void writeFields(InternalRow row) {
    int i = 0;
    while (i < this.desc.nonIndexFields().length) {
      int ordinal = this.desc.fieldIndex(this.desc.indexFields()[i].name());
      if (row.isNullAt(ordinal)) {
        // set null bit at ordinal position
      } else {
        // skip null bit, write fixed portion of value
        // write variable portion of value
      }
    }
  }

  private void flush(OutputStream out) throws IOException {
    // write all buffers into output buffer, release resources.
    this.indexedBuffer.writeTo(out);
    this.fixedBuffer.writeTo(out);
    this.variableBuffer.writeTo(out);
  }
}
