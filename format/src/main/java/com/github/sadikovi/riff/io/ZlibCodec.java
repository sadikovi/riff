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
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZlibCodec implements CompressionCodec {
  private Deflater deflater;
  private Inflater inflater;

  public ZlibCodec(int level, int strategy) {
    this.deflater = new Deflater(level, true);
    this.deflater.setStrategy(strategy);
    this.inflater = new Inflater(true);
  }

  public ZlibCodec() {
    this(Deflater.DEFAULT_COMPRESSION, Deflater.DEFAULT_STRATEGY);
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow) throws IOException {
    int length = in.remaining();
    int outSize = 0;
    deflater.reset();
    deflater.setInput(in.array(), in.arrayOffset() + in.position(), length);
    deflater.finish();
    int offset = out.arrayOffset() + out.position();
    while (!deflater.finished() && length > outSize) {
      int size = deflater.deflate(out.array(), offset, out.remaining());
      out.position(size + out.position());
      outSize += size;
      offset += size;
      // if we run out of space in the out buffer, use the overflow
      if (out.remaining() == 0) {
        if (overflow == null || overflow.remaining() == 0) {
          return false;
        }
        out = overflow;
        offset = out.arrayOffset() + out.position();
      }
    }
    return length > outSize;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    boolean hasRemaining = true;
    inflater.reset();
    inflater.setInput(in.array(), in.arrayOffset() + in.position(), in.remaining());
    while (!(inflater.finished() || inflater.needsDictionary() || inflater.needsInput())) {
      try {
        int count = inflater.inflate(out.array(), out.arrayOffset() + out.position(),
          out.remaining());
        out.position(count + out.position());
        // if we have marked buffer as having 0 bytes to insert on previous iteration, we should
        // raise an error, which means output buffer is too short and we are trying to insert into
        // already full buffer
        if (!hasRemaining) {
          throw new IOException("Output buffer is too short, could not insert more bytes from " +
            "compressed byte buffer");
        }
        hasRemaining = count == 0;
      } catch (DataFormatException dfe) {
        throw new IOException("Bad compression data", dfe);
      }
    }
    out.flip();
    in.position(in.limit());
  }

  @Override
  public void reset() {
    deflater.reset();
    inflater.reset();
  }

  @Override
  public void close() {
    if (deflater != null) {
      deflater.end();
      deflater = null;
    }
    if (inflater != null) {
      inflater.end();
      inflater = null;
    }
  }
}
