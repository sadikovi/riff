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
      return new RowIntegerConverter();
    } else if (dataType instanceof LongType) {
      return new RowLongConverter();
    } else if (dataType instanceof StringType) {
      return new RowStringConverter();
    } else {
      throw new RuntimeException("No converter registered for type " + dataType);
    }
  }

  //////////////////////////////////////////////////////////////
  // Supported row value converters
  //////////////////////////////////////////////////////////////

  public static class RowIntegerConverter extends RowValueConverter {
    @Override
    public void write(InternalRow row, int ordinal, OutputBuffer buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeInt(row.getInt(ordinal));
    }

    @Override
    public void writeFixedVar(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeInt(row.getInt(ordinal));
    }

    @Override
    public void read(InternalRow row, int ordinal, byte[] buffer) throws IOException {
      // TODO: implement read method
      throw new UnsupportedOperationException();
    }
  }

  public static class RowLongConverter extends RowValueConverter {
    @Override
    public void write(InternalRow row, int ordinal, OutputBuffer buffer) throws IOException {
      // method avoids unboxing because of internal row specialized getters
      buffer.writeLong(row.getLong(ordinal));
    }

    @Override
    public void writeFixedVar(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeLong(row.getLong(ordinal));
    }

    @Override
    public void read(InternalRow row, int ordinal, byte[] buffer) throws IOException {
      // TODO: implement read method
      throw new UnsupportedOperationException();
    }
  }

  public static class RowStringConverter extends RowValueConverter {
    @Override
    public void write(InternalRow row, int ordinal, OutputBuffer buffer) throws IOException {
      // string is written as (bytes length) -> (bytes sequence)
      byte[] bytes = row.getUTF8String(ordinal).getBytes();
      buffer.writeInt(bytes.length);
      buffer.write(bytes);
    }

    @Override
    public void writeFixedVar(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        OutputBuffer variableBuffer) throws IOException {
      byte[] bytes = row.getUTF8String(ordinal).getBytes();
      // write length into fixed buffer
      fixedBuffer.writeInt(bytes.length);
      // write content bytes into variable buffer
      variableBuffer.write(bytes);
    }

    @Override
    public void read(InternalRow row, int ordinal, byte[] buffer) throws IOException {
      // TODO: implement read method
      throw new UnsupportedOperationException();
    }
  }
}
