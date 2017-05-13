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
  public static final String UNCOMPRESSED_SHORT_NAME = "none";
  public static final String UNCOMPRESSED_FILE_EXTENSION = "";
  public static final byte UNCOMPRESSED_ENCODE_FLAG = 0;
  // constants for zlib codec
  public static final String ZLIB_FILE_EXTENSION = ".deflate";
  public static final String ZLIB_SHORT_NAME = "deflate";
  public static final byte ZLIB_ENCODE_FLAG = 1;
  // constants for gzip codec
  public static final String GZIP_FILE_EXTENSION = ".gz";
  public static final String GZIP_SHORT_NAME = "gzip";
  public static final byte GZIP_ENCODE_FLAG = 2;
  // constants for snappy codec
  public static final String SNAPPY_FILE_EXTENSION = ".snappy";
  public static final String SNAPPY_SHORT_NAME = "snappy";
  public static final byte SNAPPY_ENCODE_FLAG = 3;

  private CompressionCodecFactory() { }

  /**
   * Get encoded byte flag for compression codec.
   * @param codec compression codec
   * @return byte flag
   */
  public static byte encode(CompressionCodec codec) {
    // return flag as power of 2
    if (codec == UNCOMPRESSED) return UNCOMPRESSED_ENCODE_FLAG;
    if (codec instanceof ZlibCodec) return ZLIB_ENCODE_FLAG;
    if (codec instanceof GzipCodec) return GZIP_ENCODE_FLAG;
    if (codec instanceof SnappyCodec) return SNAPPY_ENCODE_FLAG;
    throw new UnsupportedOperationException("Unknown codec: " + codec);
  }

  /**
   * Decode flag into supported compression codec.
   * @param flag byte flag
   * @return compression codec
   */
  public static CompressionCodec decode(byte flag) {
    if (flag == UNCOMPRESSED_ENCODE_FLAG) return UNCOMPRESSED;
    // return zlib codec with default settings
    if (flag == ZLIB_ENCODE_FLAG) return new ZlibCodec();
    if (flag == GZIP_ENCODE_FLAG) return new GzipCodec();
    if (flag == SNAPPY_ENCODE_FLAG) return new SnappyCodec();
    throw new UnsupportedOperationException("Unknown codec flag: " + flag);
  }

  /**
   * Get codec for short name.
   * @param name codec name
   * @return compression codec
   */
  public static CompressionCodec forShortName(String name) {
    switch (name.toLowerCase()) {
      case ZLIB_SHORT_NAME:
        return new ZlibCodec();
      case GZIP_SHORT_NAME:
        return new GzipCodec();
      case SNAPPY_SHORT_NAME:
        return new SnappyCodec();
      case UNCOMPRESSED_SHORT_NAME:
        return UNCOMPRESSED;
      default:
        throw new UnsupportedOperationException("Unknown codec: " + name);
    }
  }

  /**
   * Get codec for file extension.
   * If extension is unknown uncompressed codec is returned.
   * @param ext file extension, e.g. ".gz", ".deflate", ".snappy"
   * @return compression codec
   */
  public static CompressionCodec forFileExt(String ext) {
    switch (ext) {
      case ZLIB_FILE_EXTENSION:
        return new ZlibCodec();
      case GZIP_FILE_EXTENSION:
        return new GzipCodec();
      case SNAPPY_FILE_EXTENSION:
        return new SnappyCodec();
      default:
        return UNCOMPRESSED;
    }
  }

  /**
   * Return file extension for provided short name that is associated with compression codec.
   * If no such name exists, exception is thrown.
   * @param name short name for codec
   * @return file extension that is associated with codec
   */
  public static String fileExtForShortName(String name) {
    switch (name.toLowerCase()) {
      case ZLIB_SHORT_NAME:
        return ZLIB_FILE_EXTENSION;
      case GZIP_SHORT_NAME:
        return GZIP_FILE_EXTENSION;
      case SNAPPY_SHORT_NAME:
        return SNAPPY_FILE_EXTENSION;
      case UNCOMPRESSED_SHORT_NAME:
        return UNCOMPRESSED_FILE_EXTENSION;
      default:
        throw new UnsupportedOperationException("Unknown codec: " + name);
    }
  }
}
