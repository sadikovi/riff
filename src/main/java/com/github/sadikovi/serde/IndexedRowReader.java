package com.github.sadikovi.serde;

import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Reader for [[IndexedRow]] instances, created per stream and reused across rows.
 */
public class IndexedRowReader {
  private final TypeDescription desc;
  // buffer for primitive fields
  private byte[] buf;
  // bit set to mark indexed fields
  private long indexed;
  // set of converters to use
  private final RowValueConverter[] converters;

  public IndexedRowReader(TypeDescription desc) {
    this.desc = desc;
    this.buf = new byte[8];
    // compute index fields in bit set
    this.indexed = 0L;
    for (TypeSpec spec : this.desc.indexFields()) {
      if (spec.isIndexed()) {
        this.indexed |= 1L << spec.position();
      }
    }
    // initialize converters, they are reused across rows
    this.converters = new RowValueConverter[this.desc.size()];
    TypeSpec[] arr = this.desc.fields();
    for (TypeSpec spec : arr) {
      this.converters[spec.position()] = Converters.sqlTypeToConverter(spec.dataType());
    }
  }

  public InternalRow readRow(InputStream in) throws IOException {
    boolean hasNulls = readByte(in) != 0;
    long bitset = 0L;
    if (hasNulls) {
      bitset = readLong(in);
    }
    IndexedRow row = new IndexedRow(this.indexed, rowOffsets(bitset));
    // set nulls for row
    row.setNulls(bitset);
    // read index region
    int indexBytes = readInt(in);
    if (indexBytes > 0) {
      byte[] indexRegion = new byte[indexBytes];
      readFully(in, indexRegion, 0, indexBytes);
      row.setIndexRegion(indexRegion, 0, indexBytes);
    }
    // read data region
    int dataBytes = readInt(in);
    if (dataBytes > 0) {
      byte[] dataRegion = new byte[dataBytes];
      readFully(in, dataRegion, 0, dataBytes);
      row.setDataRegion(dataRegion, 0, dataBytes);
    }
    return row;
  }

  private void relativeRowOffset(int[] offsets, long bitset, TypeSpec[] fields) {
    int offset = 0;
    for (int i = 0; i < fields.length; i++) {
      if ((bitset & 1L << fields[i].position()) == 0) {
        // update offset
        offsets[fields[i].position()] = offset;
        offset += this.converters[fields[i].position()].offset();
      } else {
        // set null value
        offsets[fields[i].position()] = -1;
      }
    }
  }

  private int[] rowOffsets(long bitset) {
    int[] offsets = new int[this.desc.size()];
    // update index fields
    relativeRowOffset(offsets, bitset, this.desc.indexFields());
    // update data fields
    relativeRowOffset(offsets, bitset, this.desc.dataFields());
    return offsets;
  }

  private long readLong(InputStream in) throws IOException {
    readFully(in, this.buf, 0, 8);
    return convertToLong(this.buf);
  }

  private int readInt(InputStream in) throws IOException {
    readFully(in, this.buf, 0, 4);
    return convertToInt(this.buf);
  }

  private int readByte(InputStream in) throws IOException {
    int bytes = in.read();
    if (bytes < 0) throw new EOFException();
    return bytes;
  }

  private void readFully(InputStream in, byte[] buffer, int offset, int len) throws IOException {
    if (len < 0) {
      throw new IndexOutOfBoundsException("Negative length: " + len);
    }
    while (len > 0) {
      // in.read will block until some data is available.
      int bytesRead = in.read(buffer, offset, len);
      if (bytesRead < 0) throw new EOFException();
      len -= bytesRead;
      offset += bytesRead;
    }
  }

  private static long convertToLong(byte[] buffer) {
    return
      ((long) (buffer[0] & 0xff) << 56) |
      ((long) (buffer[1] & 0xff) << 48) |
      ((long) (buffer[2] & 0xff) << 40) |
      ((long) (buffer[3] & 0xff) << 32) |
      ((long) (buffer[4] & 0xff) << 24) |
      ((long) (buffer[5] & 0xff) << 16) |
      ((long) (buffer[6] & 0xff) <<  8) |
      ((long) (buffer[7] & 0xff));
  }

  private static int convertToInt(byte[] buffer) {
    return
      ((buffer[0] & 0xff) << 24) |
      ((buffer[1] & 0xff) << 16) |
      ((buffer[2] & 0xff) << 8) |
      (buffer[3] & 0xff);
  }
}
