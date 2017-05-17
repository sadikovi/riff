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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.CompressionCodecFactory;

/**
 * [[Riff]] class is the main entrypoint of working with Riff file format.
 * It exposes two primary methods to create either writer or reader as builders with set methods
 * for different options.
 *
 * Example of writing/reading simple file:
 * {{{
 * // writing ".gz" file
 * org.apache.hadoop.conf.Configuration conf = ...
 * org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("file.gz");
 * TypeDescription td = new TypeDescription(structType, indexFields);
 * FileWriter writer = Riff.writer(conf, path, td);
 *
 * writer.prepareWrite();
 * while (rows.hasNext()) {
 *   writer.write(rows.next());
 * }
 * writer.finishWrite();
 *
 * // reading file
 * TreeNode filter = eqt("indexField", "abc");
 * FileReader reader = Riff.reader(conf, new org.apache.hadoop.fs.Path("file.gz"));
 * RowBuffer rowbuf = reader.prepareRead(filter);
 * while (rowbuf.hasNext()) {
 *   process(rowbuf.next()); // user-specific processing of an InternalRow
 * }
 * rowbuf.close();
 * }}}
 *
 * See additional methods to set options for write/read, such as enforcing compression codec,
 * specifying file system, HDFS buffer size, in/out stream buffer size, type description, etc.
 */
public class Riff {
  private static final Logger LOG = LoggerFactory.getLogger(Riff.class);

  /**
   * Internal riff options that can be set in hadoop configuration.
   */
  public static class Options {
    // short name for compression codec
    public static final String COMPRESSION_CODEC = "riff.compression.codec";

    // Number of rows in single stripe, this is used for writing only
    public static final String STRIPE_ROWS = "riff.stripe.rows";
    public static final int STRIPE_ROWS_DEFAULT = 10000;

    // buffer size in bytes
    public static final String BUFFER_SIZE = "riff.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 256 * 1024;
    public static final int BUFFER_SIZE_MIN = 4 * 1024;
    public static final int BUFFER_SIZE_MAX = 512 * 1024;

    // buffer size for Hadoop output/input stream
    public static final String HDFS_BUFFER_SIZE = "io.file.buffer.size";
    // default buffer size for HDFS, should be multiple of 4096 bytes, same as in core-default.xml
    public static final int HDFS_BUFFER_SIZE_DEFAULT = 4 * 1024;

    // whether or not column filters are enabled and should be written into header
    public static final String COLUMN_FILTER_ENABLED = "riff.column.filter.enabled";
    // column filters are enabled by default
    public static final boolean COLUMN_FILTER_ENABLED_DEFAULT = true;

    /**
     * Get compression codec from configuration.
     * If option is not set, null value is returned.
     * @param conf configuration
     * @return compression codec short name
     */
    static String compressionCodecName(Configuration conf) {
      return conf.get(COMPRESSION_CODEC);
    }

    /**
     * Select next power of 2 as buffer size.
     * @param conf configuration
     * @return validated bytes value
     */
    static int power2BufferSize(Configuration conf) {
      int bytes = conf.getInt(BUFFER_SIZE, BUFFER_SIZE_DEFAULT);
      if (bytes > BUFFER_SIZE_MAX) return BUFFER_SIZE_MAX;
      if (bytes < BUFFER_SIZE_MIN) return BUFFER_SIZE_MIN;
      // bytes is already power of 2
      if ((bytes & (bytes - 1)) == 0) return bytes;
      bytes = Integer.highestOneBit(bytes) << 1;
      return (bytes < BUFFER_SIZE_MAX) ? bytes : BUFFER_SIZE_MAX;
    }

    /**
     * Select HDFS buffer size.
     * @param conf configuration
     * @return HDFS buffer size when open or create file
     */
    static int hdfsBufferSize(Configuration conf) {
      // bytes should be multiple of hardware pages 4096
      int pageSize = 4096;
      int bytes = conf.getInt(HDFS_BUFFER_SIZE, HDFS_BUFFER_SIZE_DEFAULT);
      if (bytes > 0 && bytes % pageSize == 0) return bytes;
      if (bytes < 0) bytes = 0;
      return (bytes / pageSize + 1) * pageSize;
    }

    /**
     * Select positive number of rows in stripe.
     * @param conf configuration
     * @return number of rows in stripe, or throws exception if number is invalid
     */
    static int numRowsInStripe(Configuration conf) {
      int rows = conf.getInt(STRIPE_ROWS, STRIPE_ROWS_DEFAULT);
      // there should be positive number of rows in stripe
      if (rows < 1) {
        throw new IllegalArgumentException("Expected positive number of rows in stripe, found " +
          rows + " <= 0");
      }
      return rows;
    }

    /**
     * Select column filters (enable/disable).
     * @param conf configuration
     * @return true if column filters are enabled
     */
    static boolean columnFilterEnabled(Configuration conf) {
      return conf.getBoolean(COLUMN_FILTER_ENABLED, COLUMN_FILTER_ENABLED_DEFAULT);
    }
  }

  /**
   * Construct path to the temporary data file.
   * @param path file path
   * @return temporary data path
   */
  protected static Path makeDataPath(Path path) {
    // prefix file name with "." and append ".data" suffix
    return new Path(path.getParent(), new Path("." + path.getName() + ".data"));
  }

  /**
   * Encode compression codec into byte flag.
   * @param codec compression codec, can be null
   * @return byte encoded flag
   */
  protected static byte encodeCompressionCodec(CompressionCodec codec) {
    return CompressionCodecFactory.encode(codec);
  }

  /**
   * Decode byte encoded flag into compression codec.
   * Compression codec can be null if flag is set to 0.
   * @param flag byte flag
   * @return compression codec or null for uncompressed stream
   */
  protected static CompressionCodec decodeCompressionCodec(byte flag) {
    return CompressionCodecFactory.decode(flag);
  }

  /**
   * Infer compression codec from file name.
   * @param path path to the file
   * @return compression codec or null for uncompressed
   */
  protected static CompressionCodec inferCompressionCodec(Path path) {
    String name = path.getName();
    int start = name.lastIndexOf('.');
    String ext = (start <= 0) ? "" : name.substring(start);
    return CompressionCodecFactory.forFileExt(ext);
  }

  private Riff() { /* no-op */ }

  //////////////////////////////////////////////////////////////
  // Public API for file writer
  //////////////////////////////////////////////////////////////

  /**
   * Get new writer.
   * Compression codec, if not set, is inferred from the file path.
   * @param fs file system to use
   * @param conf configuration with Riff options
   * @param path path to write
   * @param td type description (schema)
   * @return file writer
   */
  public static FileWriter writer(
      FileSystem fs,
      Configuration conf,
      Path path,
      TypeDescription td) {
    // check if compression codec is set in configuration, otherwise fall back to the inferring
    // codec from file extension
    CompressionCodec codec;
    if (Options.compressionCodecName(conf) != null) {
      codec = CompressionCodecFactory.forShortName(Options.compressionCodecName(conf));
    } else {
      codec = inferCompressionCodec(path);
    }
    try {
      return new FileWriter(fs, conf, path, td, codec);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get new writer.
   * Compression codec, if not set, is inferred from the file path.
   * @param conf configuration with Riff options
   * @param path path to write
   * @param td type description
   * @return file writer
   */
  public static FileWriter writer(Configuration conf, Path path, TypeDescription td) {
    try {
      return writer(path.getFileSystem(conf), conf, path, td);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get new writer.
   * Compression codec, if not set, is inferred from the file path.
   * @param path path to write
   * @param td type description
   * @return file writer
   */
  public static FileWriter writer(Path path, TypeDescription td) {
    return writer(new Configuration(), path, td);
  }

  //////////////////////////////////////////////////////////////
  // Public API for file reader
  //////////////////////////////////////////////////////////////

  /**
   * Get new reader.
   * @param fs file system to use
   * @param conf configuration with Riff options
   * @param path file path to read
   * @return file reader
   */
  public static FileReader reader(FileSystem fs, Configuration conf, Path path) {
    return new FileReader(fs, conf, path);
  }

  /**
   * Get new reader.
   * @param conf configuration with Riff options
   * @param path file path to read
   * @return file reader
   */
  public static FileReader reader(Configuration conf, Path path) {
    try {
      return reader(path.getFileSystem(conf), conf, path);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get new reader.
   * @param path file path to read
   * @return file reader
   */
  public static FileReader reader(Path path) {
    return reader(new Configuration(), path);
  }

  //////////////////////////////////////////////////////////////
  // Public API for metadata write/read
  //////////////////////////////////////////////////////////////

  /**
   * Get metadata reader.
   * @param fs file system to use
   * @param conf configuration with Riff options
   * @param metadataPath path to the metadata file or directory where metadata is stored
   */
  public static Metadata.MetadataReader metadataReader(
      FileSystem fs,
      Configuration conf,
      Path metadataPath) {
    return new Metadata.MetadataReader(fs, conf, metadataPath);
  }

  /**
   * Get metadata reader.
   * @param conf configuration with Riff options
   * @param metadataPath path to the metadata file or directory where metadata is stored
   */
  public static Metadata.MetadataReader metadataReader(Configuration conf, Path metadataPath) {
    try {
      return metadataReader(metadataPath.getFileSystem(conf), conf, metadataPath);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get metadata reader.
   * @param metadataPath path to the metadata file or directory where metadata is stored
   */
  public static Metadata.MetadataReader metadataReader(Path metadataPath) {
    return metadataReader(new Configuration(), metadataPath);
  }

  /**
   * Get metadata writer.
   * @param fs file system to use
   * @param conf hadoop configuration with riff settings
   * @param filepath filepath to a valid Riff file
   */
  public static Metadata.MetadataWriter metadataWriter(
      FileSystem fs,
      Configuration conf,
      Path filepath) {
    try {
      return new Metadata.MetadataWriter(fs, conf, filepath);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get metadata writer.
   * @param conf hadoop configuration with riff settings
   * @param filepath filepath to a valid Riff file
   */
  public static Metadata.MetadataWriter metadataWriter(Configuration conf, Path filepath) {
    try {
      return metadataWriter(filepath.getFileSystem(conf), conf, filepath);
    } catch (IOException err) {
      throw new RuntimeException("Error occured: " + err.getMessage(), err);
    }
  }

  /**
   * Get metadata writer.
   * @param filepath filepath to a valid Riff file
   */
  public static Metadata.MetadataWriter metadataWriter(Path filepath) {
    return metadataWriter(new Configuration(), filepath);
  }
}
