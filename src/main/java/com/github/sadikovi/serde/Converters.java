package com.github.sadikovi.serde;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;

/**
 * Placeholder for specific implementations of row value converters.
 */
public class Converters {
  private Converters() { }

  /**
   * Select value converter for Spark SQL type. Throws exception if type is not supported.
   */
  public static RowValueConverter sqlTypeToConverter(DataType dataType) {
    if (dataType instanceof IntegerType) {
      return new IndexedRowIntConverter();
    } else if (dataType instanceof LongType) {
      return new IndexedRowLongConverter();
    } else if (dataType instanceof StringType) {
      return new IndexedRowUTF8Converter();
    } else {
      throw new RuntimeException("No converter registered for type " + dataType);
    }
  }

  //////////////////////////////////////////////////////////////
  // Supported row value converters
  //////////////////////////////////////////////////////////////

  public static class IndexedRowIntConverter extends RowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeInt(row.getInt(ordinal));
    }

    @Override
    public int offset() {
      return 4;
    }
  }

  public static class IndexedRowLongConverter extends RowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeLong(row.getLong(ordinal));
    }

    @Override
    public int offset() {
      return 8;
    }
  }

  public static class IndexedRowUTF8Converter extends RowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      byte[] bytes = row.getUTF8String(ordinal).getBytes();
      // write offset + length into fixed buffer and content into variable length buffer
      long offset = variableBuffer.bytesWritten() + fixedOffset;
      long metadata = (offset << 32) + bytes.length;
      fixedBuffer.writeLong(metadata);
      variableBuffer.writeBytes(bytes);
    }

    @Override
    public int offset() {
      // metadata size (offset + length)
      return 8;
    }
  }
}
