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

/**
 * StripeOutputBuffer keeps data in memory for entire stripe until buffer is flushed to disk.
 * Has very simple interface similar to OutputStream.
 */
public class StripeOutputBuffer {
  // stripe unique id (across stripes within file)
  private final short id;
  // total data in stripe
  private final OutputBuffer data;

  public StripeOutputBuffer(short id) {
    this.id = id;
    this.data = new OutputBuffer();
  }

  /**
   * Return id of the stripe within a file.
   * @return stripe id
   */
  public short id() {
    return id;
  }

  /**
   * Get bytes written into stripe.
   * @return length in bytes
   */
  public int length() {
    return data.bytesWritten();
  }

  /**
   * Write array of bytes into underlying buffer.
   * @param buffer data to write
   * @param offset array offset
   * @param length bytes to write
   * @throws IOException
   */
  public void write(byte[] buffer, int offset, int length) throws IOException {
    data.writeBytes(buffer, offset, length);
  }

  /**
   * Flush data in stripe buffer into provided output stream.
   * @param out external output stream
   * @throws IOException
   */
  public void flush(OutputStream out) throws IOException {
    data.writeExternal(out);
  }

  /**
   * Close resources associated with this stripe buffer.
   * @throws IOException
   */
  public void close() throws IOException {
    // output buffer close is no-op operation
    data.close();
  }

  /**
   * Return array of bytes in underlying output buffer.
   * Should be used for testing.
   */
  public byte[] array() {
    return data.array();
  }

  @Override
  public String toString() {
    return "StripeOutput[id=" + id + ", length=" + length() + "]";
  }
}
