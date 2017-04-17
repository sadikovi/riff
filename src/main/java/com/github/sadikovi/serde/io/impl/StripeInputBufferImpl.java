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

package com.github.sadikovi.serde.io.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.sadikovi.serde.io.StripeInputBuffer;

/**
 * Implementation of `StripeInputBuffer` backed by byte buffer.
 * Byte array passed as an argument is assumed to be read-only so we can safely use byte buffer on
 * top of it, it is also allowed to seek previous position (look behind).
 */
public final class StripeInputBufferImpl implements StripeInputBuffer {
  private final short id;
  private byte[] data;
  private ByteBuffer buffer;

  public StripeInputBufferImpl(short id, byte[] data) {
    this.id = id;
    this.data = data;
    this.buffer = ByteBuffer.wrap(this.data).asReadOnlyBuffer();
    this.buffer.position(0);
  }

  @Override
  public long length() {
    return data.length;
  }

  @Override
  public short id() {
    return id;
  }

  @Override
  public void seek(long position) throws IOException {
    if (position < 0 || position >= length()) throw new IndexOutOfBoundsException();
    this.buffer.position((int) position);
  }

  @Override
  public void copy(ByteBuffer out) {
    // byte buffer can only be copied if source has fewer remaining bytes, here we will have to set
    // limit on copy and restore it afterwards
    if (buffer.remaining() > out.remaining()) {
      int prevLimit = buffer.limit();
      buffer.limit(buffer.position() + out.remaining());
      out.put(buffer);
      // restore previous limit, position, however is updated
      buffer.limit(prevLimit);
    } else {
      // source has fewer bytes than out, copy data into out byte buffer
      out.put(buffer);
    }
  }

  @Override
  public void close() throws IOException {
    this.buffer.clear();
    this.buffer = null;
    this.data = null;
  }
}
