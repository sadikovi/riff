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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputBuffer is a resizable byte buffer based on `ByteArrayOutputStream` with additional methods
 * to write primitive values. Writes are optimized around writing byte arrays, which is different
 * from data output stream. Default output buffer is chosen to suit indexed row writes.
 */
public class OutputBuffer extends ByteArrayOutputStream {
  // default size in bytes
  private static final int DEFAULT_SIZE = 64;

  // buffer for primitive values
  private final byte[] buffer;

  public OutputBuffer(int size) {
    super(size);
    this.buffer = new byte[8];
  }

  /** Constructor with default size, see `ByteArrayOutputStream` for more information */
  public OutputBuffer() {
    super(DEFAULT_SIZE);
    this.buffer = new byte[8];
  }

  /**
   * How many bytes are written so far into this buffer.
   * @return number of bytes
   */
  public int bytesWritten() {
    return size();
  }

  /**
   * Return copy of output buffer's content as byte array.
   * @return copy of byte array
   */
  public byte[] array() {
    return toByteArray();
  }

  /**
   * Write current buffer into external output stream.
   * @param out sink output stream
   */
  public void writeExternal(OutputStream out) throws IOException {
    writeTo(out);
  }

  /**
   * Writes a boolean to the underlying output stream as a 1-byte value.
   * @param v boolean value to write
   */
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  /**
   * Writes out a byte to the underlying output stream as a 1-byte value.
   * @param v byte value to write
   */
  public void writeByte(int v) throws IOException {
    write(v);
  }

  /**
   * Write byte array from specified offset of specified length.
   * @param b byte array to write
   * @param off offset
   * @param len length to write
   */
  public void writeBytes(byte[] b, int off, int len) throws IOException {
    write(b, off, len);
  }

  /**
   * Write byte array with offset 0 and full length of the array.
   * @param b byte array to write
   */
  public void writeBytes(byte[] b) throws IOException {
    write(b);
  }

  /**
   * Converts the double argument to a long using the doubleToLongBits method in class Double, and
   * then writes that long value to the underlying output stream as an 8-byte quantity, high byte
   * first.
   * @param v double value to write
   */
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  /**
   * Converts the float argument to an int using the floatToIntBits method in class Float, and then
   * writes that int value to the underlying output stream as a 4-byte quantity, high byte first.
   * @param v float value to write
   */
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  /**
   * Writes an int to the underlying output stream as four bytes, high byte first.
   * @param v integer value to write
   */
  public void writeInt(int v) throws IOException {
    buffer[0] = (byte) (0xff & (v >> 24));
    buffer[1] = (byte) (0xff & (v >> 16));
    buffer[2] = (byte) (0xff & (v >>  8));
    buffer[3] = (byte) (0xff & v);
    write(buffer, 0, 4);
  }

  /**
   * Writes a long to the underlying output stream as eight bytes, high byte first.
   * @param v long value to write
   */
  public void writeLong(long v) throws IOException {
    buffer[0] = (byte) (0xff & (v >> 56));
    buffer[1] = (byte) (0xff & (v >> 48));
    buffer[2] = (byte) (0xff & (v >> 40));
    buffer[3] = (byte) (0xff & (v >> 32));
    buffer[4] = (byte) (0xff & (v >> 24));
    buffer[5] = (byte) (0xff & (v >> 16));
    buffer[6] = (byte) (0xff & (v >>  8));
    buffer[7] = (byte) (0xff & v);
    write(buffer, 0, 8);
  }

  /**
   * Writes a short to the underlying output stream as two bytes, high byte first.
   * @param v short value to write
   */
  public void writeShort(int v) throws IOException {
    buffer[0] = (byte) (0xff & (v >> 8));
    buffer[1] = (byte) (0xff & v);
    write(buffer, 0, 2);
  }
}
