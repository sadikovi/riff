package com.github.sadikovi.serde;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Row value converter provies specialized method to write non-null value into output stream.
 */
abstract class RowValueConverter {
  /**
   * Write value with either fixed or variable length into output buffer. Value is guaranteed to be
   * non-null and buffer is valid. Offset is length of fixed part, used for writing values with
   * variable part.
   */
  public abstract void writeDirect(
      InternalRow row,
      int ordinal,
      OutputBuffer fixedBuffer,
      int fixedOffset,
      OutputBuffer variableBuffer) throws IOException;

  /**
   * Fixed offset in bytes for data type, this either includes value for primitive types, or fixed
   * sized metadata (either int or long) for non-primitive types, e.g. UTF8String.
   */
  public abstract int byteOffset();

  @Override
  public boolean equals(Object other) {
    return other != null && other.getClass().equals(this.getClass());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
