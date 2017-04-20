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
import java.nio.ByteBuffer;

/**
 * Stripe input buffer.
 * Keeps information about current stripe, including total bytes in stripe, offset, and id.
 * Specification implies that stripe is loaded into memory entirely and read in parts by consumers,
 * such as `InStream`s.
 */
public class StripeInputBuffer {
  // stripe unique id (across stripes within file)
  private final short id;
  // total data in stripe
  private byte[] data;
  // current offset in data
  private int offset;

  public StripeInputBuffer(short id, byte[] data) {
    if (data == null || data.length == 0) {
      throw new IllegalArgumentException("Empty data for stripe");
    }
    this.id = id;
    this.data = data;
    this.offset = 0;
  }

  /**
   * Total number of bytes in this stripe.
   * Stripe cannot have more than 2GB of data.
   * @return number of bytes
   */
  public int length() {
    return this.data.length;
  }

  /**
   * Position of cursor within stripe: remaining bytes = length() - position().
   * @return offset within stripe
   */
  public int position() {
    return offset;
  }

  /**
   * Stripe sequential id within a file.
   * @return id as short value
   */
  public short id() {
    return this.id;
  }

  /**
   * Position cursor in input buffer to the desired position.
   * If position is negative or is larger than actual size of input buffer, exception is thrown.
   * @param position desired position in input buffer
   * @throws IOException when position is < 0 or >= length, or less than current position
   */
  public void seek(int position) throws IOException {
    if (position < 0 || position > data.length) {
      throw new IOException("Invalid position " + position + " for " + this);
    }

    if (position < offset) {
      throw new IOException("Cannot set position " + position + " < current offset " + offset);
    }
    offset = position;
  }

  /**
   * Copy data into provided byte buffer, it should have position and limit set accordingly to
   * indicate how many bytes need to transfered. Note that if this stripe input buffer contains
   * fewer bytes (potentially 0) than remaining in output buffer, output buffer will be modified to
   * reflect this change (limit will be resized, so remaining bytes are only those copied).
   * @param out buffer to copy into
   */
  public void copy(ByteBuffer out) {
    int remaining = Math.min(out.remaining(), data.length - offset);
    out.put(data, offset, remaining);
    // advance position
    offset += remaining;
    // flip buffer to make it ready for reads; if we copied fewer bytes than remaining in buffer,
    // after flip limit will be set correctly, and position will be reset to 0
    out.flip();
  }

  /**
   * Close and/or relese all resources maintained by this stripe input buffer.
   * @throws IOException
   */
  public void close() throws IOException {
    this.data = null;
    this.offset = 0;
  }

  @Override
  public String toString() {
    return "StripeInput[id=" + id + ", offset=" + offset +
      ", closed=" + (data == null) + "]";
  }
}
