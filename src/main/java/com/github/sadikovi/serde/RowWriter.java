package com.github.sadikovi.serde;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;

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
  private OutputBuffer indexedBuffer;
  // buffer to write fixed portion of row
  private OutputBuffer fixedBuffer;
  // buffer to write variable portion of row
  private OutputBuffer variableBuffer;
  // set of converters to use
  private final RowValueConverter[] converters;

  public RowWriter(TypeDescription desc) {
    this.desc = desc;
    // these byte buffers are reset and reused between serializations
    this.indexedBuffer = new OutputBuffer();
    this.fixedBuffer = new OutputBuffer();
    this.variableBuffer = new OutputBuffer();
    // initialize converters, they are reused across rows
    // converters are created for sql schema ordinals, not type description positions, because we
    // fetch data from internal row
    this.converters = new RowValueConverter[this.desc.size()];
    Iterator<TypeSpec> iter = this.desc.fields();
    while (iter.hasNext()) {
      TypeSpec spec = iter.next();
      this.converters[spec.origSQLPos()] = Converters.sqlTypeToConverter(spec.dataType());
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
      int ordinal = this.desc.indexFields()[i].origSQLPos();
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
      ++i;
    }
  }

  private void flush(OutputStream out) throws IOException {
    // write all buffers into output buffer, release resources.
    this.indexedBuffer.writeTo(out);
    this.fixedBuffer.writeTo(out);
    this.variableBuffer.writeTo(out);
  }
}
