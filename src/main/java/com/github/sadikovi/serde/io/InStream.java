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

package com.github.sadikovi.serde.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class InStream extends InputStream {
  // buffer size to use for this stream
  private final int bufferSize;
  // buffer for reading primitive types (similar to DataInputStream)
  private final byte[] buf;
  // compression codec to use, should be set to null if source is uncompressed
  private final CompressionCodec codec;
  // source stream to read from (usually represents stripe information)
  private StripeInputBuffer source;
  // byte buffer to store uncompressed part of data
  private ByteBuffer uncompressed;
  // current offset (total bytes either read or skipped) relative to the source
  private long currentOffset;

  public InStream(int bufferSize, CompressionCodec codec, StripeInputBuffer source) {
    this.bufferSize = bufferSize;
    // initialize buffer to read all primitive types (8 bytes max for long and double)
    this.buf = new byte[8];
    // codec is null if source stream does not have compressed chunks
    this.codec = codec;
    this.uncompressed = ByteBuffer.allocate(bufferSize);
    this.currentOffset = 0L;
    this.source = source;
    // fill up buffer with some data from source
    source.copy(uncompressed);
  }

  @Override
  public int available() throws IOException {
    if (uncompressed.remaining() != 0) return uncompressed.remaining();
    return (int) (source.length() - currentOffset);
  }

  /**
   * Read signed long value from buffer.
   * Method buffers source when necessary.
   * @return long value
   * @throws IOException
   */
  public long readLong() throws IOException {
    read(this.buf, 0, 8);
    return convertToLong(this.buf);
  }

  /**
   * Read signed integer value from buffer.
   * Method buffers source when necessary.
   * @return integer value
   * @throws IOException
   */
  public int readInt(InputStream in) throws IOException {
    read(this.buf, 0, 4);
    return convertToInt(this.buf);
  }

  /**
   * Read single byte from buffer.
   * Method buffers source when necessary.
   * @return byte value as integer
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    read(this.buf, 0, 1);
    return this.buf[0] & 0xff;
  }

  /**
   * Convert provided byte array into long, expected first 8 bytes available.
   */
  private static long convertToLong(byte[] buffer) {
    return
      ((long) (buffer[0] & 0xff) << 56) |
      ((long) (buffer[1] & 0xff) << 48) |
      ((long) (buffer[2] & 0xff) << 40) |
      ((long) (buffer[3] & 0xff) << 32) |
      ((long) (buffer[4] & 0xff) << 24) |
      ((long) (buffer[5] & 0xff) << 16) |
      ((long) (buffer[6] & 0xff) << 8) |
      ((long) (buffer[7] & 0xff));
  }

  /**
   * Convert provided byte array into integer, expected first 4 bytes available.
   */
  private static int convertToInt(byte[] buffer) {
    return
      ((buffer[0] & 0xff) << 24) |
      ((buffer[1] & 0xff) << 16) |
      ((buffer[2] & 0xff) << 8) |
      (buffer[3] & 0xff);
  }

  /**
   * Read method copies bytes from underlying buffer into provided array. If necessary, it will
   * buffer source for more data. If last batch is being read, fewer bytes might be returned, but,
   * otherwise, it will ensure reading all bytes that were requested.
   * @param bytes byte array to copy into
   * @param offset offset
   * @param length length to copy
   * @return number of bytes actually being copied into array
   * @throws IOException
   */
  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (length < 0) {
      throw new IndexOutOfBoundsException("Negative length: " + length);
    }
    int bytesRead = Math.min(uncompressed.remaining(), length);
    uncompressed.put(bytes, offset, bytesRead);
    // keep track of maximum number of bytes we have copied into array
    int bytesSoFar = bytesRead;
    while (length > 0) {
      // refill buffer
      uncompressed.clear();
      source.copy(uncompressed);

      offset += bytesRead;
      bytesRead = Math.min(uncompressed.remaining(), length);
      // no more bytes can be read, EOF is reached
      if (bytesRead == 0) return bytesSoFar;
      uncompressed.put(bytes, offset, bytesRead);
      length -= bytesRead;
    }
    return bytesSoFar;
  }

  /**
   * Skip number of bytes.
   * If offset is within current buffer, position is updated, otherwise source is asked to advance
   * k number of bytes and new buffer is allocated.
   * @param bytes number of bytes to skip
   * @return actual number of bytes skipped
   * @throws IOException
   */
  @Override
  public long skip(long bytes) throws IOException {
    // if we have some bytes left in buffer, just shift position, but only if we have some remaining
    // bytes left after this operation
    if (uncompressed.remaining() > bytes) {
      uncompressed.position(uncompressed.position() + (int) bytes);
      return bytes;
    }
    // at this point, we are short on bytes to skip, we need to indicate to source that we require
    // seeking to new offset and copy bytes from there. Here we do not care about bytes left in
    // uncompressed buffer.
    currentOffset += uncompressed.position() + bytes;
    uncompressed.clear();
    if (source.length() > currentOffset) {
      // all bytes can be skipped, we seek to the new position, copy remaining bytes from source
      // and return skipped bytes
      source.seek(currentOffset);
      source.copy(uncompressed);
      return bytes;
    } else {
      // seek to the end of the stream, we would have to skip only those bytes
      source.seek(source.length() - 1);
      // no copy, just set limit to 0, so no remaining bytes are left
      uncompressed.limit(0);
      return bytes - (currentOffset - source.length());
    }
  }

  @Override
  public void close() throws IOException {
    // release resources
    this.source.close();
    this.source = null;
    this.currentOffset = 0;
    this.uncompressed = null;
  }

  @Override
  public void mark(int readLimit) {
    throw new UnsupportedOperationException("mark/reset is not supported");
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void reset() throws IOException {
    throw new UnsupportedOperationException("mark/reset is not supported");
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[bufferSize=" + bufferSize + ", codec=" + codec + "]";
  }
}
