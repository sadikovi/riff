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

/**
 * Compression codec interface.
 */
public interface CompressionCodec {
  /**
   * Compress the in buffer to the out buffer.
   * @param in the bytes to compress
   * @param out the uncompressed bytes
   * @param overflow put any additional bytes here
   * @return true if the output is smaller than input
   * @throws IOException
   */
  boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow) throws IOException;

  /**
   * Decompress the in buffer to the out buffer.
   * Input buffer is flipped and set to start position for reading.
   * Output buffer is already flipped to start position and ready for writing.
   * @param in the bytes to decompress
   * @param out the decompressed bytes
   * @throws IOException
   */
  void decompress(ByteBuffer in, ByteBuffer out) throws IOException;

  /** Reset the codec, preparing it for reuse */
  void reset();

  /** Close the codec, releasing the resources */
  void close();
}
