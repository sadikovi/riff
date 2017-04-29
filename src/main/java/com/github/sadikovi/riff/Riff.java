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

import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.GzipCodec;
import com.github.sadikovi.riff.io.ZlibCodec;

public class Riff {
  private static final Logger LOG = LoggerFactory.getLogger(Riff.class);

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
    public static final String BUFFER_SIZE = "riff.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 256 * 1024;
    public static final int BUFFER_SIZE_MIN = 4 * 1024;
    public static final int BUFFER_SIZE_MAX = 512 * 1024;

    // buffer size for Hadoop output/input stream
    public static final String HDFS_BUFFER_SIZE = "io.file.buffer.size";
    // default buffer size for HDFS, should be multiple of 4096 bytes, same as in core-default.xml
    public static final int HDFS_BUFFER_SIZE_DEFAULT = 4 * 1024;

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
    if (codec instanceof GzipCodec) return 2;
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
    if (flag == 2) return new GzipCodec();
    throw new UnsupportedOperationException("Unknown codec flag: " + flag);
  }

  /**
   * Base builder class.
   * Provides access to set most of the options. For additional specific to write/read options see
   * either [[WriterBuilder]] or [[ReaderBuilder]].
   */
  protected static abstract class Builder<T, R> {
    // instance to return
    protected T instance;
    // file system to use
    protected FileSystem fs;
    // internal configuration, used to set riff options, is not intended to hold other hadoop
    // settings, but it can be set as such. It is recommended to use external hadoop configuration
    // to initialize file system
    protected Configuration conf;

    protected Builder() {
      this.fs = null;
      this.conf = new Configuration();
    }

    /**
     * Set file system.
     * @param fs file system, must not be null
     * @return this instance
     */
    public T setFileSystem(FileSystem fs) {
      if (fs == null) throw new NullPointerException("File system is null");
      this.fs = fs;
      return this.instance;
    }

    /**
     * Set configuration.
     * This replaces current instance configuration.
     * @param conf configuration, must not be null
     * @return this instance
     */
    public T setConf(Configuration conf) {
      if (conf == null) throw new NullPointerException("Configuration is null");
      this.conf = conf;
      return this.instance;
    }

    /**
     * Set buffer size in bytes.
     * @param bytes buffer size
     * @return this instance
     */
    public T setBufferSize(int bytes) {
      this.conf.setInt(Options.BUFFER_SIZE, bytes);
      return this.instance;
    }

    /**
     * Set buffer size for Hadoop input or output stream.
     * @param bytes buffer size
     * @return this instance
     */
    public T setHadoopStreamBufferSize(int bytes) {
      this.conf.setInt("io.file.buffer.size", bytes);
      return this.instance;
    }

    /**
     * Set number of rows in stripe.
     * @param rows number of rows
     * @return this instance
     */
    public T setRowsInStripe(int rows) {
      this.conf.setInt(Options.STRIPE_ROWS, rows);
      return this.instance;
    }

    /**
     * Create final instance of either writer or reader depending on param type for provided path.
     * @param path path to a file for specified file system
     * @return T instance
     * @throws IOException if any IO error occurs
     */
    public abstract R create(Path path) throws IOException;
  }

  /**
   * Writer settings builder.
   */
  public static class WriterBuilder extends Builder<WriterBuilder, FileWriter> {
    private CompressionCodec codec;
    private boolean codecSet;
    private TypeDescription td;

    protected WriterBuilder() {
      super();
      this.instance = this;
      this.codec = null;
      this.codecSet = false;
      this.td = null;
    }

    /**
     * Force compression codec for writer.
     * @param codecName string name of the codec {DEFLATE, NONE}
     * @return this instance
     */
    public WriterBuilder setCodec(String codecName) {
      switch (codecName.toLowerCase()) {
        case "deflate":
          this.codec = new ZlibCodec();
          break;
        case "gzip":
          this.codec = new GzipCodec();
          break;
        case "none":
          this.codec = null;
          break;
        default:
          throw new UnsupportedOperationException("Unknown codec: " + codecName);
      }
      this.codecSet = true;
      return this;
    }

    /**
     * Force compression codec for writer.
     * @param codec codec to use, can be null
     * @return this instance
     */
    public WriterBuilder setCodec(CompressionCodec codec) {
      // codec can be null
      this.codec = codec;
      this.codecSet = true;
      return this;
    }

    /**
     * Set type description for writer.
     * @param td type description to use, must not be null
     * @return this instance
     */
    public WriterBuilder setTypeDesc(TypeDescription td) {
      if (td == null) throw new NullPointerException("Type description is null");
      this.td = td;
      return this;
    }

    /**
     * Set type description using Spark SQL schema and list of index fields that should be
     * indexed in this schema.
     * @param schema Spark SQL schema
     * @param indexFields list of potential index fields
     * @return this instance
     */
    public WriterBuilder setTypeDesc(StructType schema, String... indexFields) {
      return setTypeDesc(new TypeDescription(schema, indexFields));
    }

    /**
     * Set type description using Spark SQL schema.
     * This method does not set any index fields for schema.
     * @param schema Spark SQL schema
     * @return this instance
     */
    public WriterBuilder setTypeDesc(StructType schema) {
      return setTypeDesc(new TypeDescription(schema));
    }

    /**
     * Infer compression codec from file name.
     * @param path path to the file
     * @return compression codec or null for uncompressed
     */
    protected CompressionCodec inferCompressionCodec(Path path) {
      String name = path.getName();
      if (name.endsWith(".deflate")) return new ZlibCodec();
      if (name.endsWith(".gz")) return new GzipCodec();
      // return null for uncompressed
      return null;
    }

    @Override
    public FileWriter create(Path path) throws IOException {
      // if codec is not set infer from path
      if (!codecSet) {
        codec = inferCompressionCodec(path);
      }
      // set file system if none found
      if (fs == null) {
        fs = path.getFileSystem(conf);
      }
      // type description is required
      if (td == null) {
        throw new RuntimeException("Type description is not set");
      }
      FileWriter writer = new FileWriter(fs, conf, path, td, codec);
      LOG.info("Created writer {}", writer);
      return writer;
    }
  }

  /**
   * Reader settings builder.
   */
  public static class ReaderBuilder extends Builder<ReaderBuilder, FileReader> {
    protected ReaderBuilder() {
      super();
      this.instance = this;
    }

    @Override
    public FileReader create(Path path) throws IOException {
      // set file system if none found
      if (fs == null) {
        fs = path.getFileSystem(conf);
      }
      FileReader reader = new FileReader(fs, conf, path);
      LOG.info("Created reader {}", reader);
      return reader;
    }
  }

  private Riff() { /* no-op */ }

  /**
   * Get new writer.
   * @return writer builder
   */
  public static WriterBuilder writer() {
    return new WriterBuilder();
  }

  /**
   * Get new reader.
   * @return reader builder
   */
  public static ReaderBuilder reader() {
    return new ReaderBuilder();
  }
}
