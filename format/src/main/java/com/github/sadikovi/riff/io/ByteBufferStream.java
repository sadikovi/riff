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

/**
 * Input stream backed by byte buffer.
 * Simply keeps track of position in byte buffer, underlying byte buffer is updated accordingly.
 */
public class ByteBufferStream extends InputStream {
  // underlying byte buffer as source
  private final ByteBuffer buf;

  public ByteBufferStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int available() {
    return buf.remaining();
  }

  @Override
  public int read() {
    return buf.hasRemaining() ? (buf.get() & 0xff) : -1;
  }

  @Override
  public int read(byte[] bytes) {
    return read(bytes, 0, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) {
    // method does not return negative bytes when no bytes are available
    int len = Math.min(buf.remaining(), length);
    buf.get(bytes, offset, len);
    return len;
  }

  @Override
  public long skip(long bytes) {
    if (buf.remaining() > bytes) {
      buf.position(buf.position() + (int) bytes);
      return bytes;
    } else {
      int len = buf.remaining();
      buf.position(buf.limit());
      return len;
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    /* no-op */
  }
}
