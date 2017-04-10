package com.github.sadikovi.serde;

import java.io.ObjectOutputStream;
import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Row value writer provies specialized method to write non-null value into output stream.
 */
abstract interface RowValueWriter {
  /**
   * Write index value from internal row into output stream. Value is guaranteed to be non-null
   * and stream is guaranteed to be correct; do not close stream after writing.
   */
  public void write(InternalRow row, int ordinal, ObjectOutputStream buffer) throws IOException;
}
