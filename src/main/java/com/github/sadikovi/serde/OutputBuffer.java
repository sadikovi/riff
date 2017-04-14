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

package com.github.sadikovi.serde;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Resizable output buffer built on top of `java.io.ByteArrayOutputStream`.
 * Provides some common methods to write primitives into stream.
 */
public class OutputBuffer extends ByteArrayOutputStream {
  // wrapper stream to provide common functionality of writing bytes
  private DataOutputStream stream;

  public OutputBuffer(int size) {
    super(size);
    this.stream = new DataOutputStream(this);
  }

  /** Constructor with default size, see `ByteArrayOutputStream` for more information */
  public OutputBuffer() {
    super();
    this.stream = new DataOutputStream(this);
  }

  /**
   * Reset byte array stream and refresh wrapper output stream.
   */
  @Override
  public void reset() {
    super.reset();
    this.stream = new DataOutputStream(this);
  }

  /**
   * How many bytes are written so far into this buffer.
   * @return number of bytes
   */
  public int bytesWritten() {
    return this.stream.size();
  }

  /**
   * Return content of output buffer as byte array.
   * @return byte array - content of buffer
   */
  public byte[] array() {
    return super.toByteArray();
  }

  /**
   * Writes a boolean to the underlying output stream as a 1-byte value.
   * @param v boolean value to write
   */
  public void writeBoolean(boolean v) throws IOException {
    this.stream.writeBoolean(v);
  }

  /**
   * Writes out a byte to the underlying output stream as a 1-byte value.
   * @param v byte value to write
   */
  public void writeByte(int v) throws IOException {
    this.stream.writeByte(v);
  }

  /**
   * Converts the double argument to a long using the doubleToLongBits method in class Double, and
   * then writes that long value to the underlying output stream as an 8-byte quantity, high byte
   * first.
   * @param v double value to write
   */
  public void writeDouble(double v) throws IOException {
    this.stream.writeDouble(v);
  }

  /**
   * Converts the float argument to an int using the floatToIntBits method in class Float, and then
   * writes that int value to the underlying output stream as a 4-byte quantity, high byte first.
   * @param v float value to write
   */
  public void writeFloat(float v) throws IOException {
    this.stream.writeFloat(v);
  }

  /**
   * Writes an int to the underlying output stream as four bytes, high byte first.
   * @param v integer value to write
   */
  public void writeInt(int v) throws IOException {
    this.stream.writeInt(v);
  }

  /**
   * Writes a long to the underlying output stream as eight bytes, high byte first.
   * @param v long value to write
   */
  public void writeLong(long v) throws IOException {
    this.stream.writeLong(v);
  }

  /**
   * Writes a short to the underlying output stream as two bytes, high byte first.
   * @param v short value to write
   */
  public void writeShort(int v) throws IOException {
    this.stream.writeShort(v);
  }

  /**
   * Write byte array from specified offset of specified length.
   * @param b byte array to write
   * @param off offset
   * @param len length to write
   */
  public void writeBytes(byte[] b, int off, int len) throws IOException {
    this.stream.write(b, off, len);
  }

  /**
   * Write byte array with offset 0 and full length of the array.
   * @param b byte array to write
   */
  public void writeBytes(byte[] b) throws IOException {
    this.stream.write(b);
  }
}
