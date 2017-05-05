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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Gzip compression codec.
 */
public class GzipCodec implements CompressionCodec {
  // assign default gzip buffer size as initial size
  private static final int GZIP_BUFFER_SIZE = 4096;
  private OutputBuffer buffer;

  public GzipCodec() {
    this.buffer = new OutputBuffer(GZIP_BUFFER_SIZE);
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow) throws IOException {
    int length = in.remaining();
    buffer.reset();
    GZIPOutputStream compressedStream = new GZIPOutputStream(buffer, GZIP_BUFFER_SIZE);
    compressedStream.write(in.array(), in.arrayOffset() + in.position(), length);
    compressedStream.finish();
    byte[] data = buffer.array();
    // if compressed data is larger than raw data, return false without updating out buffer
    if (length <= data.length) {
      out.position(out.limit());
      overflow.position(overflow.limit());
      return false;
    }
    int pos = 0;
    while (pos < data.length) {
      int len = Math.min(data.length - pos, out.remaining());
      System.arraycopy(data, pos, out.array(), out.arrayOffset() + out.position(), len);
      out.position(len + out.position());
      if (out.remaining() == 0) {
        if (overflow == null || overflow.remaining() == 0) {
          return false;
        }
        out = overflow;
      }
      pos += len;
    }
    return length > data.length;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(in.array(),
      in.arrayOffset() + in.position(), in.remaining());
    GZIPInputStream compressedStream = new GZIPInputStream(stream, GZIP_BUFFER_SIZE);
    int bytes = 0;
    while (bytes != -1) {
      if (out.remaining() == 0) {
        bytes = compressedStream.read();
      } else {
        bytes = compressedStream.read(out.array(), out.arrayOffset() + out.position(),
          out.remaining());
      }
      if (bytes != -1) {
        // if out buffer has 0 bytes space left, but inflater has some bytes to decompress, throw an
        // exception because of output buffer being too short; this is similar to zlibcodec
        if (out.remaining() == 0) {
          throw new IOException("Output buffer is too short, could not insert more bytes from " +
            "compressed byte buffer");
        }
        out.position(bytes + out.position());
      }
    }
    compressedStream.close();
    out.flip();
    in.position(in.limit());
  }

  @Override
  public void reset() {
    buffer.reset();
  }

  @Override
  public void close() {
    buffer = null;
  }
}
