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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.ZlibCodec;

public class Riff {
  public static final String MAGIC = "RIFF";
  // suffix for data files
  public static final String DATA_FILE_SUFFIX = ".data";

  /**
   * Internal riff options that can be set in hadoop configuration.
   */
  public static class Options {
    // Number of rows in single stripe, this is used for writing only
    public static final String STRIPE_ROWS = "riff.stripe.rows";
    public static final int STRIPE_ROWS_DEFAULT = 10000;

    // buffer size in bytes
    // TODO: link to the "io.file.buffer.size" setting for hadoop output stream
    public static final String BUFFER_SIZE = "riff.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 256 * 1024;
    public static final int BUFFER_SIZE_MIN = 4 * 1024;
    public static final int BUFFER_SIZE_MAX = 512 * 1024;

    /**
     * Select next power of 2 as buffer size.
     * @param conf configuration
     * @return validated bytes value
     */
    static int power2BufferSize(Configuration conf) {
      int bytes = conf.getInt(BUFFER_SIZE, BUFFER_SIZE_DEFAULT);
      if (bytes > Riff.Options.BUFFER_SIZE_MAX) return Riff.Options.BUFFER_SIZE_MAX;
      if (bytes < Riff.Options.BUFFER_SIZE_MIN) return Riff.Options.BUFFER_SIZE_MIN;
      // bytes is already power of 2
      if ((bytes & (bytes - 1)) == 0) return bytes;
      bytes = Integer.highestOneBit(bytes) << 1;
      return (bytes < Riff.Options.BUFFER_SIZE_MAX) ? bytes : Riff.Options.BUFFER_SIZE_MAX;
    }

    /**
     * Select positive number of rows in stripe.
     * @param conf configuration
     * @return number of rows in stripe, or throws exception if number is invalid
     */
    static int numRowsInStripe(Configuration conf) {
      int rows = conf.getInt(Riff.Options.STRIPE_ROWS, Riff.Options.STRIPE_ROWS_DEFAULT);
      // there should be positive number of rows in stripe
      if (rows < 1) {
        throw new IllegalArgumentException("Expected positive number of rows in stripe, found " +
          rows + " <= 0");
      }
      return rows;
    }
  }

  /**
   * Append data file suffix to the path, suffix is always the last block in file name.
   * @param path header path
   * @return data path
   */
  static Path makeDataPath(Path path) {
    return path.suffix(DATA_FILE_SUFFIX);
  }

  /**
   * Encode compression codec into byte flag. If codec is null return 0 for uncompressed flag.
   * Flag should always be positive.
   * @param codec compression codec, can be null
   * @return byte encoded flag
   */
  static byte encodeCompressionCodec(CompressionCodec codec) {
    // uncompressed stream
    if (codec == null) return 0;
    if (codec instanceof ZlibCodec) return 1;
    throw new UnsupportedOperationException("Unknown codec: " + codec);
  }

  /**
   * Decode byte encoded flag into compression codec. Compression codec can be null if flag is set
   * to 0. Flag should always be positive.
   * @param flag byte flag
   * @return compression codec or null for uncompressed stream
   */
  static CompressionCodec decodeCompressionCodec(byte flag) {
    // uncompressed stream
    if (flag == 0) return null;
    // return zlib codec with default settings
    if (flag == 1) return new ZlibCodec();
    throw new UnsupportedOperationException("Unknown codec flag: " + flag);
  }

  private Riff() { }
}
