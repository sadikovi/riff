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

import org.xerial.snappy.Snappy;

public class SnappyCodec implements CompressionCodec {
  // snappy size for temporary buffer
  private static final int SNAPPY_BUFFER_SIZE = 4096;
  // temporary byte array to reuse between compress calls
  private byte[] buffer;

  public SnappyCodec() {
    // snappy buffer size is default size, it is reallocated if we need larger buffer
    this.buffer = new byte[SNAPPY_BUFFER_SIZE];
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow) throws IOException {
    // find out approximate number of bytes we need for compression, note that this number can be
    // higher than input size, but actual compressed bytes are smaller than input size
    int compressedBytes = Snappy.maxCompressedLength(in.remaining());
    // resize buffer if necessary
    if (buffer.length < compressedBytes) {
      buffer = new byte[compressedBytes];
    }
    // compress into allocated array and update compressed bytes
    compressedBytes = Snappy.compress(in.array(), in.arrayOffset() + in.position(),
      in.remaining(), buffer, 0);
    // check if it is still better to keep data compressed, this check is done assuming that in
    // buffer is not modified until this point
    if (compressedBytes >= in.remaining()) {
      out.position(out.limit());
      if (overflow != null) {
        overflow.position(overflow.limit());
      }
      return false;
    } else {
      // copy bytes into out and overflow buffers
      int len = Math.min(compressedBytes, out.remaining());
      out.put(buffer, 0, len);
      compressedBytes -= len;
      if (compressedBytes > 0) {
        if (overflow == null || overflow.remaining() == 0) return false;
        overflow.put(buffer, len, compressedBytes);
      }
      return true;
    }
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    int uncompressedBytes = Snappy.uncompressedLength(in.array(), in.arrayOffset() + in.position(),
      in.remaining());
    if (uncompressedBytes > out.remaining()) {
      throw new IOException("Output buffer is too short, could not insert more bytes from " +
        "compressed byte buffer");
    }
    // this does not update positions in in or out buffers, we will have to do manually after
    // decompression.
    uncompressedBytes = Snappy.uncompress(in.array(), in.arrayOffset() + in.position(),
      in.remaining(), out.array(), out.arrayOffset() + out.position());
    out.position(out.position() + uncompressedBytes);
    // prepare for read
    out.flip();
    in.position(in.limit());
  }

  @Override
  public void reset() {
    // no-op
  }

  @Override
  public void close() {
    buffer = null;
  }
}
