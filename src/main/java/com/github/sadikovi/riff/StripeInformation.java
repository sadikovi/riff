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

import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.io.StripeOutputBuffer;

/**
 * Interface for single stripe.
 * Contains information for reader.
 */
public class StripeInformation {
  private static final String MAGIC = "STRIPE";

  private final short id;
  private final long offset;
  private final int length;

  public StripeInformation(StripeOutputBuffer stripe, long pos) {
    this.id = stripe.id();
    this.offset = pos;
    this.length = stripe.length();
  }

  /**
   * Get stripe id within a file.
   * @return stripe id
   */
  public short id() {
    return this.id;
  }

  /**
   * Get offset in bytes within a file.
   * @return stripe offset
   */
  public long offset() {
    return this.offset;
  }

  /**
   * Get stripe length in bytes.
   * @return bytes for stripe
   */
  public int length() {
    return this.length;
  }

  /**
   * Write stripe information into external stream.
   * @param buffer output buffer
   * @throws IOException
   */
  public void writeExternal(OutputBuffer buffer) throws IOException {
    // we write:
    // - magic for stripe (6 bytes)
    // - stripe id (2 bytes)
    // - offset (8 bytes)
    // - length (4 bytes)
    buffer.writeBytes(MAGIC.getBytes());
    buffer.writeShort(id());
    buffer.writeLong(offset());
    buffer.writeInt(length());
  }

  @Override
  public String toString() {
    return "Stripe[id=" + id + ", offset=" + offset + ", length=" + length + "]";
  }
}
