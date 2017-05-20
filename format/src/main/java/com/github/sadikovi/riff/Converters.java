/*
 * Copyright (c) 2017 sadikovi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.sadikovi.riff;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;

import com.github.sadikovi.riff.io.OutputBuffer;

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
    } else if (dataType instanceof DateType) {
      return new IndexedRowIntConverter();
    } else if (dataType instanceof TimestampType) {
      return new IndexedRowLongConverter();
    } else if (dataType instanceof BooleanType) {
      return new IndexedRowBooleanConverter();
    } else if (dataType instanceof ShortType) {
      return new IndexedRowShortConverter();
    } else if (dataType instanceof ByteType) {
      return new IndexedRowByteConverter();
    } else {
      throw new RuntimeException("No converter registered for type " + dataType);
    }
  }

  //////////////////////////////////////////////////////////////
  // Supported row value converters
  //////////////////////////////////////////////////////////////

  static abstract class IndexedRowValueConverter implements RowValueConverter {
    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof IndexedRowValueConverter)) return false;
      if (other == this) return true;
      return this.getClass().equals(other.getClass());
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  public static class IndexedRowIntConverter extends IndexedRowValueConverter {
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
    public int byteOffset() {
      return 4;
    }
  }

  public static class IndexedRowLongConverter extends IndexedRowValueConverter {
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
    public int byteOffset() {
      return 8;
    }
  }

  public static class IndexedRowUTF8Converter extends IndexedRowValueConverter {
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
    public int byteOffset() {
      // metadata size (offset + length)
      return 8;
    }
  }

  public static class IndexedRowBooleanConverter extends IndexedRowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeBoolean(row.getBoolean(ordinal));
    }

    @Override
    public int byteOffset() {
      // boolean is written into single byte
      return 1;
    }
  }

  public static class IndexedRowShortConverter extends IndexedRowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeShort(row.getShort(ordinal));
    }

    @Override
    public int byteOffset() {
      return 2;
    }
  }

  public static class IndexedRowByteConverter extends IndexedRowValueConverter {
    @Override
    public void writeDirect(
        InternalRow row,
        int ordinal,
        OutputBuffer fixedBuffer,
        int fixedOffset,
        OutputBuffer variableBuffer) throws IOException {
      fixedBuffer.writeByte(row.getByte(ordinal));
    }

    @Override
    public int byteOffset() {
      return 1;
    }
  }
}
