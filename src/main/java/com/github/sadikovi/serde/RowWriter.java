package com.github.sadikovi.serde;

import java.io.DataOutputStream;
import java.io.IOException;

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
  // null bit set for indexed fields
  private NullBitSet indexedNullSet;
  // null bit set for non-indexed fields
  private NullBitSet fixedNullSet;
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
    // null bit sets
    this.indexedNullSet = new NullBitSet(this.desc.indexFields().length);
    this.fixedNullSet = new NullBitSet(this.desc.nonIndexFields().length);
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
  public void writeRow(InternalRow row, DataOutputStream out) throws IOException {
    // reset current buffers
    prepareWrite();
    writeIndexedFields(row);
    writeFields(row);
    flush(out);
  }

  private void prepareWrite() throws IOException {
    // reset null bits
    this.indexedNullSet.clear();
    this.fixedNullSet.clear();
    // reset temporary buffers and state
    this.indexedBuffer.reset();
    this.fixedBuffer.reset();
    this.variableBuffer.reset();
  }

  /**
   * Indexed fields are written directly, in the format:
   * +---------------+------------------------+--------------+--------------+-----+--------------+
   * | is_nulls byte | optional nulls bit set | value1 bytes | value2 bytes | ... | valueN bytes |
   * +---------------+------------------------+--------------+--------------+-----+--------------+
   * For variable length fields are written:
   * +----------------+-------------------+
   * | length 4 bytes | sequence of bytes |
   * +----------------+-------------------+
   * If null bit is set, no bytes are written for values. Optional nulls bit set can be excluded
   */
  private void writeIndexedFields(InternalRow row) throws IOException {
    // extract indexed fields and write them into temporary buffer
    int i = 0;
    while (i < this.desc.indexFields().length) {
      int ordinal = this.desc.indexFields()[i].origSQLPos();
      if (row.isNullAt(ordinal)) {
        this.indexedNullSet.setBit(i);
      } else {
        this.converters[ordinal].write(row, ordinal, this.indexedBuffer);
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
   * For fields with variable length bytes are written:
   * +----------------+     +-------------------+
   * | length 4 bytes | ... | sequence of bytes |
   * +----------------+     +-------------------+
   * If null bit is set, nothing is written at field position.
   */
  private void writeFields(InternalRow row) throws IOException {
    int i = 0;
    while (i < this.desc.nonIndexFields().length) {
      int ordinal = this.desc.nonIndexFields()[i].origSQLPos();
      if (row.isNullAt(ordinal)) {
        this.fixedNullSet.setBit(i);
      } else {
        this.converters[ordinal].
          writeFixedVar(row, ordinal, this.fixedBuffer, this.variableBuffer);
      }
      ++i;
    }
  }

  /**
   * Flush content of buffers and null bit sets into output stream.
   * No need to reset buffers, this code should be done in `prepareWrite` method.
   * Bytes are written in certain order:
   * +---------------+-----------------------+----------------+
   * | is_nulls byte | optional null bit set | indexed buffer |
   * +---------------+-----------------------+----------------+
   * ...
   * +----------------------------------------+---------------+-----------------------+
   * | record size (excluding indexed fields) | is_nulls byte | optional null bit set |
   * +----------------------------------------+---------------+-----------------------+
   * ...
   * +------------------+--------------+-----------------+
   * | fixed chunk size | fixed buffer | variable buffer |
   * +------------------+--------------+-----------------+
   */
  private void flush(DataOutputStream out) throws IOException {
    // write index part
    if (this.indexedNullSet.isEmpty()) {
      out.write(0x0);
    } else {
      out.write(0x1);
      this.indexedNullSet.writeTo(out);
    }
    this.indexedBuffer.writeTo(out);
    // write values part
    // record size = 1 null byte + optional compressed bytes of null bit set + fixed size integer +
    // fixed buffer size + variable buffer size
    boolean isNullSet = this.fixedNullSet.isEmpty();
    int recordSize = 1 + (isNullSet ? 0 : this.fixedNullSet.compressedBytes());
    recordSize += 4 + this.fixedBuffer.bytesWritten() + this.variableBuffer.bytesWritten();
    out.writeInt(recordSize);
    if (isNullSet) {
      out.write(0x0);
    } else {
      out.write(0x1);
      this.fixedNullSet.writeTo(out);
    }
    // write fixed size
    out.writeInt(this.fixedBuffer.bytesWritten());
    // write buffers
    this.fixedBuffer.writeTo(out);
    this.variableBuffer.writeTo(out);
  }
}
