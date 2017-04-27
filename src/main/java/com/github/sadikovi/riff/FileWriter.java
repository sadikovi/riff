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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.catalyst.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.io.OutStream;
import com.github.sadikovi.riff.io.StripeOutputBuffer;

/**
 * File writer provides methods to prepare files and write rows into data file. It creates two
 * files at the end of the operation: one is header file containing all metadata about stripes,
 * offsets and statistics in the stream and another is data file containing raw bytes of data.
 * This class should be used per thread, is not thread-safe.
 *
 * Usage:
 * {{{
 * Iterator<InternalRow> rows = ...;
 * writer.prepareWrite();
 * while (rows.hasNext()) {
 *   writer.write(rows.next());
 * }
 * writer.finishWrite();
 * }}}
 *
 * Writer should be used only to create file once, reuses are not allowed - create a new
 * instance instead. Multiple calls of `prepareWrite` are allowed and result in no-op, the same
 * goes for `finishWrite` method. When `finishWrite` is called, writer flushes the last stripe and
 * creates header file for the already written data file.
 */
class FileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);
  // suffix for data files
  private static final String DATA_FILE_SUFFIX = ".data";

  // file system to use for writing a file
  private final FileSystem fs;
  // resolved path for header file
  private final Path headerPath;
  // resolved path for data file
  private final Path dataPath;
  // type description for schema to write
  private final TypeDescription td;
  // unique id for this writer, assigned per file
  private final byte[] fileId;
  // number of rows in stripe
  private final int numRowsInStripe;
  // buffer size for outstream
  private final int bufferSize;
  // compression codec, can be null
  private final CompressionCodec codec;

  // write has been prepared
  private boolean writePrepared;
  // write has been finished
  private boolean writeFinished;
  // output stream, either for data file or header file
  private FSDataOutputStream out;
  // stripe id (incremented for each stripe)
  private short stripeId;
  // all stripes in a data file
  private ArrayList<StripeInformation> stripes;
  // record writer
  private IndexedRowWriter recordWriter;
  // current stripe output buffer
  private StripeOutputBuffer stripe;
  // current stripe out stream
  private OutStream stripeStream;
  // number of records in current stripe
  private int stripeCurrentRecords;
  // statistics per stripe
  private Statistics[] stripeStats;

  /**
   * Create file writer for path.
   * Configuration is passed separately and not reused from `fs.getConf`. This is to be explicit
   * about separate configuration from most of the hadoop settings. Actual user-facing API will
   * allow providing configuration for both file system and internal options.
   * @param fs file system to use
   * @param conf configuration
   * @param path path to the header file, also used to create data path
   * @param td type description for rows
   * @param codec compression codec
   * @throws IOException
   * @throws FileAlreadyExistsException
   */
  FileWriter(
      FileSystem fs,
      Configuration conf,
      Path path,
      TypeDescription td,
      CompressionCodec codec) throws IOException {
    this.fs = fs;
    this.headerPath = fs.makeQualified(path);
    this.dataPath = makeDataPath(headerPath);
    this.writePrepared = false;
    this.writeFinished = false;
    if (this.fs.exists(headerPath)) {
      throw new FileAlreadyExistsException("Already exists: " + headerPath);
    }
    if (this.fs.exists(dataPath)) {
      throw new FileAlreadyExistsException(
        "Data path already exists: " + dataPath + ". Data path is created from provided path " +
        path + " and also should not exist when creating writer");
    }
    // this assumes that subsequent rows are provided for this schema
    this.td = td;
    // generate unique id for this file, collisions are possible, this mainly to prevent accidental
    // renaming of files
    this.fileId = nextFileKey();
    this.numRowsInStripe =
      conf.getInt(Riff.Options.STRIPE_ROWS, Riff.Options.STRIPE_ROWS_DEFAULT);
    if (numRowsInStripe < 1) {
      throw new IllegalArgumentException(
        "Expected positive number of rows in stripe, found " + numRowsInStripe);
    }
    this.bufferSize = power2BufferSize(
      conf.getInt(Riff.Options.BUFFER_SIZE, Riff.Options.BUFFER_SIZE_DEFAULT));
    this.codec = codec;
  }

  /**
   * Append data file suffix to the path, suffix is always the last block in file name.
   * @param path header path
   * @return data path
   */
  private static Path makeDataPath(Path path) {
    return path.suffix(DATA_FILE_SUFFIX);
  }

  /**
   * Generate new file key of 12 bytes for file identifier.
   * This key is shared between header and data files.
   * @return array with random byte values
   */
  private static byte[] nextFileKey() {
    Random rand = new Random();
    byte[] key = new byte[12];
    rand.nextBytes(key);
    return key;
  }

  /**
   * Select next power of 2 as buffer size.
   * @param bytes initial bytes value
   * @return validated bytes value
   */
  private static int power2BufferSize(int bytes) {
    if (bytes > Riff.Options.BUFFER_SIZE_MAX) return Riff.Options.BUFFER_SIZE_MAX;
    if (bytes < Riff.Options.BUFFER_SIZE_MIN) return Riff.Options.BUFFER_SIZE_MIN;
    // bytes is already power of 2
    if ((bytes & (bytes - 1)) == 0) return bytes;
    bytes = Integer.highestOneBit(bytes) << 1;
    return (bytes < Riff.Options.BUFFER_SIZE_MAX) ? bytes : Riff.Options.BUFFER_SIZE_MAX;
  }

  /**
   * Return header path for writer.
   * @return header path
   */
  public Path headerPath() {
    return headerPath;
  }

  /**
   * Return data path for writer.
   * @return data path
   */
  public Path dataPath() {
    return dataPath;
  }

  /**
   * Number of rows in stripe for this writer.
   * @return positive number of rows
   */
  public int numRowsInStripe() {
    return numRowsInStripe;
  }

  /**
   * Return selected buffer size this is used for outstream instances.
   * @return buffer size as power of 2
   */
  public int bufferSize() {
    return bufferSize;
  }

  /**
   * Write global header that is shared between header file and data file.
   * @param out output stream to write into
   * @throws IOException
   */
  private void writeHeader(FSDataOutputStream out) throws IOException {
    // header consists of 16 bytes - 4 bytes magic and 12 bytes unique key
    OutputBuffer dataHeader = new OutputBuffer();
    dataHeader.writeBytes(Riff.MAGIC.getBytes());
    dataHeader.writeBytes(fileId);
    if (dataHeader.bytesWritten() != 16) {
      throw new IOException("Invalid number of bytes written - expected 16, got " +
        dataHeader.bytesWritten());
    }
    dataHeader.writeExternal(out);
  }

  /**
   * Prepare writer. This method initializes stripes, statistics and counters.
   * @throws IOException
   */
  public void prepareWrite() throws IOException {
    if (writeFinished) throw new IOException("Writer reuse");
    if (writePrepared) return;
    LOG.info("Prepare file's type description {}", td);
    stripeId = 0;
    stripes = new ArrayList<StripeInformation>();
    recordWriter = new IndexedRowWriter(td);
    LOG.info("Initialized record writer {}", recordWriter);
    // initialize stripe related parameters
    stripe = new StripeOutputBuffer(stripeId++);
    stripeStream = new OutStream(bufferSize, codec, stripe);
    stripeStats = createStatistics(td);
    stripeCurrentRecords = numRowsInStripe;
    LOG.info("Initialize stripe outstream {}", stripeStream);
    // create stream for data file
    try {
      out = fs.create(dataPath, false);
      LOG.info("Prepare data file header");
      writeHeader(out);
    } catch (IOException ioe) {
      if (out != null) {
        out.close();
      }
      throw ioe;
    }
    // mark as initialized
    writePrepared = true;
  }

  /**
   * Write internal row using this writer.
   * Row should confirm to the type description.
   * @param row internal row
   * @throws IOException
   */
  public void write(InternalRow row) throws IOException {
    try {
      if (stripeCurrentRecords == 0) {
        // flush data into stripe buffer
        stripeStream.flush();
        // write stripe information into output, such as
        // stripe id and length, and capture position
        StripeInformation stripeInfo = new StripeInformation(stripe, out.getPos(), stripeStats);
        stripe.flush(out);
        LOG.info("Finished writing stripe {}, records={}", stripeInfo, numRowsInStripe);
        stripes.add(stripeInfo);
        stripe = new StripeOutputBuffer(stripeId++);
        stripeStream = new OutStream(bufferSize, codec, stripe);
        stripeCurrentRecords = numRowsInStripe;
        stripeStats = createStatistics(td);
      }
      updateStatistics(stripeStats, td, row);
      recordWriter.writeRow(row, stripeStream);
      stripeCurrentRecords--;
    } catch (IOException ioe) {
      if (out != null) {
        out.close();
      }
      throw ioe;
    }
  }

  /**
   * Finish writes.
   * All buffers are flushed at the end of this operation and streams are closed.
   * @throws IOException
   */
  public void finishWrite() throws IOException {
    if (!writePrepared) throw new IOException("Writer is not prepared");
    if (writeFinished) return;
    try {
      // flush the last stripe into output stream
      stripeStream.flush();
      StripeInformation stripeInfo = new StripeInformation(stripe, out.getPos(), stripeStats);
      stripe.flush(out);
      LOG.info("Finished writing stripe {}, records={}", stripeInfo,
        numRowsInStripe - stripeCurrentRecords);
      stripes.add(stripeInfo);
      stripeStream.close();
      stripe = null;
      stripeStream = null;
      stripeStats = null;
      // finished writing data file
      out.close();
      LOG.info("Finished writing data file {}", dataPath);

      // write header file
      LOG.info("Prepare header file");
      out = fs.create(headerPath, false);
      writeHeader(out);
      // == file content ==
      // combine all statistics for a file
      LOG.info("Merge stripe statistics");
      Statistics[] fileStats = createStatistics(td);
      for (StripeInformation info : stripes) {
        if (info.hasStatistics()) {
          for (int i = 0; i < fileStats.length; i++) {
            fileStats[i].merge(info.getStatistics()[i]);
          }
        }
      }
      LOG.info("Write header file content");
      OutputBuffer buffer = new OutputBuffer();
      // write type description and stripe information
      td.writeExternal(buffer);
      // write file statistics
      buffer.writeInt(fileStats.length);
      for (int i = 0; i < fileStats.length; i++) {
        LOG.info("Writing file statistics {}", fileStats[i]);
        fileStats[i].writeExternal(buffer);
      }
      // write stripe information
      LOG.info("Writing {} stripes", stripes.size());
      for (StripeInformation info : stripes) {
        info.writeExternal(buffer);
      }
      // flush buffer into output stream
      buffer.writeExternal(out);
      out.close();
      LOG.info("Finished writing header file {}", headerPath);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    writeFinished = true;
  }

  /**
   * Create new array of statistics for a stripe.
   * @return statistics
   */
  private static Statistics[] createStatistics(TypeDescription td) {
    Statistics[] stats = new Statistics[td.fields().length];
    for (int i = 0; i < td.fields().length; i++) {
      stats[i] = Statistics.sqlTypeToStatistics(td.fields()[i].dataType());
    }
    return stats;
  }

  /**
   * Update each instance of statistics with value of internal row.
   * @param stats list of statistics
   * @param td type description for a row
   * @param row row to use for updates, should match type description
   */
  private static void updateStatistics(Statistics[] stats, TypeDescription td, InternalRow row) {
    for (int i = 0; i < stats.length; i++) {
      // ordinal is original sql position in struct type for internal row, not index of type spec
      stats[i].update(row, td.atPosition(i).origSQLPos());
    }
  }

  @Override
  public String toString() {
    return "FileWriter[" +
      "header=" + headerPath +
      ", data=" + dataPath +
      ", type_desc=" + td +
      ", rows_per_stripe=" + numRowsInStripe +
      ", is_compressed=" + (codec != null) +
      ", buffer_size=" + bufferSize + "]";
  }
}
