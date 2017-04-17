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
 * Interface for stripe input buffer.
 * Keeps information about current stripe, including total bytes in stripe, offset, and id.
 * Specification implies that stripe is loaded into memory entirely and read in parts by consumers,
 * such as `InStream`s.
 */
public interface StripeInputBuffer {
  /**
   * Total number of bytes in this stripe.
   * @return number of bytes
   */
  long length();

  /**
   * Stripe sequential id within a file.
   * @return id as short value
   */
  short id();

  /**
   * Position cursor in input buffer to the desired position.
   * If position is negative or is larger than actual size of input buffer, exception is thrown.
   * @param position desired position in input buffer
   * @throws IOException when position is < 0 or >= length
   */
  void seek(long position) throws IOException;

  /**
   * Copy data into provided byte buffer, it should have position and limit set accordingly to
   * indicate how many bytes need to transfered. Note that if this stripe input buffer contains
   * fewer bytes (potentially 0) than remaining in output buffer, output buffer will be modified to
   * reflect this change (limit will be resized, so remaining bytes are only those copied).
   * @param out buffer to copy into
   */
  void copy(ByteBuffer out);

  /**
   * Close and/or relese all resources maintained by this stripe input buffer.
   * @throws IOException
   */
  void close() throws IOException;
}
