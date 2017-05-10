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
import java.io.OutputStream;

public class FSAppendStream extends OutputStream {
  // output stream as destination
  private OutputStream out;
  // temporary buffer that is used to copy bytes directly
  private byte[] copyBuffer;

  public FSAppendStream(OutputStream out, int copyBufferSize) {
    if (copyBufferSize < 0) {
      throw new IllegalArgumentException("Negative buffer size: " + copyBufferSize);
    }
    this.out = out;
    this.copyBuffer = new byte[copyBufferSize];
  }

  /**
   * Write stream into output stream. This merely copies bytes from source into destination and
   * buffers if possible.
   * @param in input stream to transfer
   * @throws IOException
   */
  public void writeStream(InputStream in) throws IOException {
    // TODO: make async read/write
    int copiedBytes = 0;
    while (in.available() > 0) {
      copiedBytes = in.read(copyBuffer, 0, copyBuffer.length);
      // we are done transferring bytes
      if (copiedBytes == 0) return;
      out.write(copyBuffer, 0, copiedBytes);
    }
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] bytes, int offset, int len) throws IOException {
    out.write(bytes, offset, len);
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    out.write(bytes);
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    out.close();
    copyBuffer = null;
    out = null;
  }

  @Override
  public String toString() {
    return "FSAppendStream(destination=" + out + ", bufferSize=" + copyBuffer.length + ")";
  }
}
