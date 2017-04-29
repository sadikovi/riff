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

/**
 * [[CompressionCodecFactory]] manages all available codecs including uncompressed.
 * If pool is implemented it should be part of compression codec factory.
 */
public class CompressionCodecFactory {
  // alias for uncompressed strategy
  public static final CompressionCodec UNCOMPRESSED = null;

  private CompressionCodecFactory() { }

  /**
   * Get encoded byte flag for compression codec.
   * @param codec compression codec
   * @return byte flag
   */
  public static byte encode(CompressionCodec codec) {
    // return flag as power of 2
    if (codec == UNCOMPRESSED) return 0;
    if (codec instanceof ZlibCodec) return 1;
    if (codec instanceof GzipCodec) return 2;
    throw new UnsupportedOperationException("Unknown codec: " + codec);
  }

  /**
   * Decode flag into supported compression codec.
   * @param flag byte flag
   * @return compression codec
   */
  public static CompressionCodec decode(byte flag) {
    if (flag == 0) return UNCOMPRESSED;
    // return zlib codec with default settings
    if (flag == 1) return new ZlibCodec();
    if (flag == 2) return new GzipCodec();
    throw new UnsupportedOperationException("Unknown codec flag: " + flag);
  }

  /**
   * Get codec for short name.
   * @param name codec name
   * @return compression codec
   */
  public static CompressionCodec forShortName(String name) {
    switch (name.toLowerCase()) {
      case "deflate":
        return new ZlibCodec();
      case "gzip":
        return new GzipCodec();
      case "none":
        return UNCOMPRESSED;
      default:
        throw new UnsupportedOperationException("Unknown codec: " + name);
    }
  }

  /**
   * Get codec for file extension.
   * If extension is unknown uncompressed codec is returned.
   * @param ext file extension, e.g. ".gz", ".deflate"
   * @return compression codec
   */
  public static CompressionCodec forFileExt(String ext) {
    switch (ext) {
      case ".deflate":
        return new ZlibCodec();
      case ".gz":
        return new GzipCodec();
      default:
        return UNCOMPRESSED;
    }
  }
}
