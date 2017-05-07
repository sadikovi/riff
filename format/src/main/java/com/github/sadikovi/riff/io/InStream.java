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

package com.github.sadikovi.riff.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class InStream extends InputStream {
  // buffer size to use for this stream
  private final int bufferSize;
  // buffer for reading primitive types (similar to DataInputStream)
  private final byte[] buf;
  // buffer for header values (primitive types), similar to "buf"
  private final ByteBuffer header;
  // compression codec to use, should be set to null if source is uncompressed
  private final CompressionCodec codec;
  // source stream to read from (usually represents stripe information)
  private StripeInputBuffer source;
  // byte buffer to store uncompressed part of data
  private ByteBuffer uncompressed;

  public InStream(
      int bufferSize,
      CompressionCodec codec,
      StripeInputBuffer source) throws IOException {
    this.bufferSize = bufferSize;
    // initialize buffer to read all primitive types (8 bytes max for long and double)
    this.buf = new byte[8];
    // initialize header buffer of 4 bytes
    this.header = ByteBuffer.allocate(4);
    // codec is null if source stream does not have compressed chunks
    this.codec = codec;
    this.source = source;
    this.uncompressed = ByteBuffer.allocate(bufferSize);
    readChunk();
  }

  /**
   * Depending on compression codec, read directly into uncompressed buffer or use compressed buffer
   * to either decompress bytes or reset to uncompressed depending on chunk header, since we do not
   * know if chunk is compressed or not; also ensure header consistency.
   *
   * This method assumes that uncompressed buffer is empty and needs buffering.
   */
  private void readChunk() throws IOException {
    uncompressed.clear();
    if (codec == null) {
      source.copy(uncompressed);
    } else {
      assert OutStream.HEADER_SIZE == 4: "Inconsistent header";
      // for header size of 4 bytes; use predefined header buffer and reset it for each chunk
      header.clear();
      source.copy(header);
      if (header.remaining() < OutStream.HEADER_SIZE) {
        throw new IOException("EOF, expected at least " + OutStream.HEADER_SIZE + " bytes");
      }
      // read header
      int info = header.getInt();
      boolean isCompressed = (info & (1 << 31)) != 0;
      int chunkLength = info & ~(1 << 31);
      // copy raw bytes (either compressed or uncompressed) into compressed buffer, we should be able
      // to just swap it in uncompressed case
      ByteBuffer compressed = ByteBuffer.allocate(chunkLength);
      // fewer bytes can be read from stream
      source.copy(compressed);
      if (isCompressed) {
        // reset uncompressed buffer and decompress bytes, if uncompressed buffer has smaller than
        // bufferSize - reset it to bufferSize, otherwise just clear position and limit
        // TODO: resize uncompressed buffer in case chunkLength results in more than bufferSize bytes
        // right now it will throw exception, if this situation happens
        if (uncompressed.limit() < bufferSize) {
          uncompressed = ByteBuffer.allocate(bufferSize);
        }
        codec.decompress(compressed, uncompressed);
        // at this point uncompressed buffer is set for reading, no need to flip
      } else {
        // just swap byte buffers
        uncompressed = compressed;
      }
    }
  }

  @Override
  public int available() throws IOException {
    if (uncompressed.remaining() != 0) return uncompressed.remaining();
    return source.length() - source.position();
  }

  /**
   * Read signed long value from buffer.
   * Method buffers source when necessary.
   * @return long value
   * @throws IOException
   */
  public long readLong() throws IOException {
    int bytes = read(this.buf, 0, 8);
    if (bytes != 8) throw new IOException("EOF, " + bytes + " bytes != 8");
    return convertToLong(this.buf);
  }

  /**
   * Read signed integer value from buffer.
   * Method buffers source when necessary.
   * @return integer value
   * @throws IOException
   */
  public int readInt() throws IOException {
    int bytes = read(this.buf, 0, 4);
    if (bytes != 4) throw new IOException("EOF, " + bytes + " bytes != 4");
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
    int bytes = read(this.buf, 0, 1);
    if (bytes != 1) throw new IOException("EOF, " + bytes + " bytes != 1");
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
    uncompressed.get(bytes, offset, bytesRead);
    // keep track of maximum number of bytes we have copied into array
    int bytesSoFar = length;
    length -= bytesRead;
    while (length > 0) {
      // refill buffer
      readChunk();

      offset += bytesRead;
      bytesRead = Math.min(uncompressed.remaining(), length);
      // no more bytes can be read, EOF is reached
      if (bytesRead == 0) return bytesSoFar - length;
      uncompressed.get(bytes, offset, bytesRead);
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
    if (bytes < 0) {
      throw new IndexOutOfBoundsException("Negative length: " + bytes);
    }

    // if we have some bytes left in buffer, just shift position, but only if we have some remaining
    // bytes left after this operation
    if (uncompressed.remaining() >= bytes) {
      uncompressed.position(uncompressed.position() + (int) bytes);
      return bytes;
    }
    // at this point, we are short on bytes to skip, we need to indicate to source that we require
    // seeking to new offset, after this refill buffer
    if (codec == null) {
      // for uncompressed input stream, we just reposition pointer in source
      int nextPosition = source.position() + (int) (bytes - uncompressed.remaining());
      if (nextPosition > source.length()) {
        // at this point we reached EOF, just set it, so next copy will result in 0 copied bytes
        bytes = source.length() - source.position();
        source.seek(source.length());
      } else {
        bytes = nextPosition - source.position();
        source.seek(nextPosition);
      }
      readChunk();
      return bytes;
    } else {
      // for compressed input stream, we will have to read bytes in order to determine skip position
      int diff = (int) (bytes - uncompressed.remaining());
      while (source.length() > source.position()) {
        readChunk();
        if (uncompressed.remaining() > diff) {
          uncompressed.position(uncompressed.position() + diff);
          // we can skip all requested bytes, return the same number
          return bytes;
        }
        diff -= uncompressed.remaining();
      }
      // at this point source enountered EOF, return read bytes so far
      return bytes - diff;
    }
  }

  @Override
  public void close() throws IOException {
    // release resources
    this.source.close();
    this.source = null;
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
