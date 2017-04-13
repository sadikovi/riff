package com.github.sadikovi.serde;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Row value converter provies specialized method to write non-null value into output stream and
 * read values from buffer into internal row.
 */
abstract class RowValueConverter {
  /**
   * Write index value from internal row into output buffer. Value is guaranteed to be non-null
   * and buffer is guaranteed to be correct; do not clear buffer after writing.
   */
  public abstract void write(InternalRow row, int ordinal, OutputBuffer buffer) throws IOException;

  /**
   * Write value with either fixed or variable length into output buffer. Value is guaranteed to be
   * non-null and buffer is valid.
   */
  public abstract void writeFixedVar(
      InternalRow row,
      int ordinal,
      OutputBuffer fixedBuffer,
      OutputBuffer variableBuffer) throws IOException;

  /**
   * Read value from buffer and insert into field with ordinal in internal row.
   */
  public abstract void read(InternalRow row, int ordinal, byte[] buffer) throws IOException;

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return other != null && other.getClass().equals(this.getClass());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
