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
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Internal output stream implementation to support uncompressed and compressed writes. Also has
 * an extension similar to DataOutputStream to write primitives and provides buffering.
 *
 * Stream keeps data in memory in byte buffer and does occasional spills once buffer is full.
 * Depending on result of compression it can decide to either keep data compressed or fall back to
 * uncompressed raw bytes, if size is smaller. Stream is designed to keep entire stripe in memory
 * and flush it to disk once it is fully created.
 */
public class OutStream extends OutputStream {
  // header size in bytes, header is used to store information for each written chunk about
  // compressed data
  public static final int HEADER_SIZE = 4;

  // buffer size for this stream
  private final int bufferSize;
  // buffer for writing primitive types (similar to DataOutputStream)
  private final byte[] buf;
  // buffer for uncompressed raw bytes
  private final ByteBuffer uncompressed;
  // optional compression codec, should be set to null if no compression
  private final CompressionCodec codec;
  // optional byte buffer for compressed data, only set if codec is set
  private final ByteBuffer compressed;
  // optional overflow buffer that is used to store some bytes left from compression if main
  // compressed buffer is full, generally it should fit all bytes left
  private final ByteBuffer overflow;
  // receiver output stream
  private final OutputStream receiver;

  public OutStream(int bufferSize, CompressionCodec codec, OutputStream receiver) {
    if (receiver == null || receiver instanceof OutStream) {
      throw new IllegalArgumentException("Invalid receiver stream " + receiver);
    }
    this.bufferSize = bufferSize;
    // initialize buffer to accomodate all primitive values (8 bytes max for long or double)
    this.buf = new byte[8];
    this.codec = codec;
    if (codec == null) {
      // when there is no codec, ho header is written
      this.uncompressed = ByteBuffer.allocate(bufferSize);
      this.compressed = null;
      this.overflow = null;
    } else {
      // we need to insert some root for header into uncompressed stream - this is done so when we
      // decide to use raw bytes over compressed data we would still be able to write header into
      // uncompressed buffer (normally it would be written into compressed buffer)
      this.uncompressed = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      this.uncompressed.position(HEADER_SIZE);
      this.compressed = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      this.overflow = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
    }
    this.receiver = receiver;
  }

  /**
   * Write single byte (lower value bits) into output stream.
   * @param b byte value
   * @throws IOException
   */
  @Override
  public void write(int value) throws IOException {
    buf[0] = (byte) (0xff & value);
    write(buf, 0, 1);
  }

  /**
   * Write integer value into output stream. Value is written in big endian.
   * @param value integer value
   * @throws IOException
   */
  public void writeInt(int value) throws IOException {
    buf[0] = (byte) (0xff & value >> 24);
    buf[1] = (byte) (0xff & value >> 16);
    buf[2] = (byte) (0xff & value >> 8);
    buf[3] = (byte) (0xff & value);
    write(buf, 0, 4);
  }

  /**
   * Write long value into output stream. Value is written in big endian.
   * @param value long value
   * @throws IOException
   */
  public void writeLong(long value) throws IOException {
    buf[0] = (byte) (0xff & value >> 56);
    buf[1] = (byte) (0xff & value >> 48);
    buf[2] = (byte) (0xff & value >> 40);
    buf[3] = (byte) (0xff & value >> 32);
    buf[4] = (byte) (0xff & value >> 24);
    buf[5] = (byte) (0xff & value >> 16);
    buf[6] = (byte) (0xff & value >> 8);
    buf[7] = (byte) (0xff & value);
    write(buf, 0, 8);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    int bytesWritten = Math.min(uncompressed.remaining(), length);
    // write bytes that will fit into this buffer
    uncompressed.put(bytes, offset, bytesWritten);
    length -= bytesWritten;
    while (length > 0) {
      // there are some bytes left in input array that we need to transfer into buffer
      // spill current buffer and insert remaining bytes
      spill();
      // for input array we need to shift offset, see below:
      // |_|_|_|_|_|_|_|
      //  ^-offset, length = 7
      // we wrote 2 bytes from array:
      // |X|X|_|_|_|_|_|
      //  ^-offset, length = 5 (after decrement)
      // shift offset and write next portion of bytes (or all of the remaining)
      // |X|X|_|_|_|_|_|
      //      ^-offset, length = 5
      // iterate over remaining bytes shifting offset, decrementing length, and spilling buffer
      // until all bytes will have been written into stream
      offset += bytesWritten;
      bytesWritten = Math.min(uncompressed.remaining(), length);
      uncompressed.put(bytes, offset, bytesWritten);
      length -= bytesWritten;
    }
  }

  @Override
  public void flush() throws IOException {
    // spill whatever bytes are left in buffer
    spill();
    uncompressed.clear();
    if (codec != null) {
      compressed.clear();
      overflow.clear();
    }
  }

  @Override
  public void close() throws IOException {
    // close output receiver
    this.receiver.close();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[bufferSize=" + bufferSize + ", codec=" + codec + "]";
  }

  /**
   * Write header into buffer, this includes total bytes per chunk and whether or not chunk is
   * compressed. Data should be written at provided position up to HEADER_SIZE.
   * @param buf buffer to write into
   * @param pos start position
   * @param bytes total bytes per chunk
   * @param isCompressed true if chunk is compressed, false otherwise
   */
  protected static void writeHeader(ByteBuffer buf, int pos, int bytes, boolean isCompressed) {
    if (bytes < 0) throw new IllegalArgumentException("Invalid bytes " + bytes);
    // we write 4 bytes - bytes is expected to be non-negative signed integer, we insert
    // information about compression as highest bit
    bytes |= (isCompressed) ? (1 << 31) : 0;
    buf.putInt(pos, bytes);
  }

  /**
   * Spill content of uncompressed buffer into underlying output receiver (output stream).
   * Note that spill method can be called on empty buffer that does not require spill.
   * Otherwise force spill buffer even if it is not full.
   * @throws IOException
   */
  private void spill() throws IOException {
    // buffer is empty, just ignore
    if (uncompressed.position() == 0) {
      return;
    }
    // select mode to spill
    if (codec == null) {
      // mark position as 0 and limit as current position, so we can read from buffer
      uncompressed.flip();
      // codec is not provided - we deal with uncompressed stream, just write raw bytes from
      // buffer into receiver stream, and reset buffer
      receiver.write(uncompressed.array(), uncompressed.arrayOffset() + uncompressed.position(),
        uncompressed.remaining());
      uncompressed.clear();
    } else {
      // we deal with compressed stream
      // prepare for reads - flip uncompressed buffer and set position to HEADER_SIZE
      uncompressed.flip(); // position will be 0
      uncompressed.position(HEADER_SIZE);
      int length = uncompressed.remaining(); // length of bytes
      // prepare compressed buffer; in general compressed buffer will have data left from previous
      // spill - this already has header written, which is not going to be altered. This new batch
      // will have new header, so make room for it
      int prevPos = compressed.position();
      compressed.position(prevPos + HEADER_SIZE);
      if (codec.compress(uncompressed, compressed, overflow)) {
        // compression is okay and smaller than uncompressed bytes
        // reset uncompressed
        uncompressed.clear();
        uncompressed.position(HEADER_SIZE);
        // at this point we work with compressed stream only
        // compute total bytes written with this `compress` call, prevPos does not include header
        int totalBytes = compressed.position() - prevPos - HEADER_SIZE;
        // append any leftover bytes in overflow buffer
        totalBytes += overflow.position();
        // write header for compressed buffer
        writeHeader(compressed, prevPos, totalBytes, true);
        // compressed stream should always have enough bytes to write header, if not - spill to
        // receiver, otherwise just do not do anything.
        // There are several situations why event mentioned above can happen:
        // 1. compressed stream is full up to the last byte, overflow is empty
        // 2. compressed stream is almost full, but remaining is still less than HEADER_SIZE,
        // overflow is empty
        // 3. compressed stream is full, and there are some bytes in overflow
        //
        // either way it looks safer to swap buffers and clear overflow
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          receiver.write(compressed.array(), compressed.arrayOffset() + compressed.position(),
            compressed.remaining());
          // swap buffers (copy content) and clear/reset overflow
          overflow.flip();
          compressed.put(overflow);
          overflow.clear();
        }
      } else {
        // compression is okay, but compressed data is larger than uncompressed raw data
        // in this case we just store uncompressed data, marking header accordingly

        // before doing that, check if there are some bytes left in compressed buffer, which belong
        // to previous spill; clear compressed and overflow buffers
        if (prevPos != 0) {
          compressed.position(prevPos);
          compressed.flip();
          receiver.write(compressed.array(), compressed.arrayOffset() + compressed.position(),
            compressed.remaining());
        }
        compressed.clear();
        overflow.clear();

        uncompressed.position(0); // beginning of the buffer and header
        // write uncompressed data into receiver stream, and update header
        writeHeader(uncompressed, 0, uncompressed.limit() - HEADER_SIZE, false);
        receiver.write(uncompressed.array(), uncompressed.arrayOffset() + uncompressed.position(),
          uncompressed.remaining());
        // reset uncompressed
        uncompressed.clear();
        uncompressed.position(HEADER_SIZE);
      }
    }
  }
}
