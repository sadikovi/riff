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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.catalyst.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.column.ColumnFilter;
import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.FSAppendStream;
import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.io.OutStream;
import com.github.sadikovi.riff.io.StripeOutputBuffer;
import com.github.sadikovi.riff.stats.Statistics;

/**
 * File writer provides methods to prepare files and write rows into riff data file. It creates two
 * files at the end of the operation: temporary data file that is removed once all data has been
 * copied into riff file, and actual riff file containing all metadata about stripes, offsets and
 * statistics in the stream and raw bytes of data.
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
 * creates riff file for the already written temporary data file.
 */
public class FileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

  // file system to use for writing a file
  private final FileSystem fs;
  // resolved path for final Riff file
  private final Path filePath;
  // resolved path for temporary data file
  private final Path tmpDataPath;
  // type description for schema to write
  private final TypeDescription td;
  // number of rows in stripe
  private final int numRowsInStripe;
  // buffer size for outstream
  private final int bufferSize;
  // HDFS buffer size for creating stream
  private final int hdfsBufferSize;
  // whether or not column filters are enabled
  private final boolean columnFilterEnabled;
  // compression codec, can be null
  private final CompressionCodec codec;

  // write has been prepared
  private boolean writePrepared;
  // write has been finished
  private boolean writeFinished;
  // temporary data file output stream
  private FSDataOutputStream temporaryStream;
  // file output stream
  private FSDataOutputStream outputStream;
  // stripe id (incremented for each stripe)
  private short stripeId;
  // current position in the stream
  private long currentOffset;
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
  // column filters per stripe
  private ColumnFilter[] stripeFilters;

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
    this.filePath = fs.makeQualified(path);
    this.tmpDataPath = Riff.makeDataPath(filePath);
    this.writePrepared = false;
    this.writeFinished = false;
    if (this.fs.exists(filePath)) {
      throw new FileAlreadyExistsException("Already exists: " + filePath);
    }
    if (this.fs.exists(tmpDataPath)) {
      throw new FileAlreadyExistsException(
        "Temporary data path already exists: " + tmpDataPath + ". Data path is created from " +
        "provided path " + path + " and also should not exist when creating writer");
    }
    // this assumes that subsequent rows are provided for this schema
    this.td = td;
    this.numRowsInStripe = Riff.Options.numRowsInStripe(conf);
    this.bufferSize = Riff.Options.power2BufferSize(conf);
    this.hdfsBufferSize = Riff.Options.hdfsBufferSize(conf);
    this.columnFilterEnabled = Riff.Options.columnFilterEnabled(conf);
    this.codec = codec;
    // current stripe stats and filters
    this.stripeStats = null;
    this.stripeFilters = null;
  }

  /**
   * Return file path for writer.
   * @return qualified file path
   */
  public Path filePath() {
    return filePath;
  }

  /**
   * Return temporary data path for writer.
   * @return qualified temporary data path
   */
  public Path temporaryDataPath() {
    return tmpDataPath;
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
   * Get current compression codec.
   * @return compression codec or null for uncompressed
   */
  public CompressionCodec codec() {
    return codec;
  }

  /**
   * Prepare writer. This method initializes stripes, statistics and counters.
   * @throws IOException
   */
  public void prepareWrite() throws IOException {
    if (writeFinished) throw new IOException("Writer reuse");
    if (writePrepared) return;
    LOG.debug("Prepare file writer {}", this);
    stripeId = 0;
    currentOffset = 0L;
    stripes = new ArrayList<StripeInformation>();
    recordWriter = new IndexedRowWriter(td);
    LOG.debug("Initialized record writer {}", recordWriter);
    // initialize stripe related parameters
    stripe = new StripeOutputBuffer(stripeId++);
    stripeStream = new OutStream(bufferSize, codec, stripe);
    stripeStats = createStatistics(td);
    stripeFilters = createColumnFilters(td, columnFilterEnabled, numRowsInStripe);
    stripeCurrentRecords = numRowsInStripe;
    LOG.debug("Initialize stripe outstream {}", stripeStream);
    // create stream for data file
    try {
      temporaryStream = fs.create(tmpDataPath, false, hdfsBufferSize);
    } catch (IOException ioe) {
      if (temporaryStream != null) {
        temporaryStream.close();
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
        // write stripe information into output, such as stripe id and length, and capture position
        StripeInformation stripeInfo =
          new StripeInformation(stripe, currentOffset, stripeStats, stripeFilters);
        currentOffset += stripeInfo.length();
        stripe.flush(temporaryStream);
        LOG.debug("Finished writing stripe {}, records={}", stripeInfo, numRowsInStripe);
        stripes.add(stripeInfo);
        stripe = new StripeOutputBuffer(stripeId++);
        stripeStream = new OutStream(bufferSize, codec, stripe);
        stripeCurrentRecords = numRowsInStripe;
        stripeStats = createStatistics(td);
      }
      updateStatistics(stripeStats, td, row);
      updateColumnFilters(stripeFilters, td, row);
      recordWriter.writeRow(row, stripeStream);
      stripeCurrentRecords--;
    } catch (IOException ioe) {
      if (temporaryStream != null) {
        temporaryStream.close();
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
      StripeInformation stripeInfo =
        new StripeInformation(stripe, currentOffset, stripeStats, stripeFilters);
      stripe.flush(temporaryStream);
      LOG.debug("Finished writing stripe {}, records={}", stripeInfo,
        numRowsInStripe - stripeCurrentRecords);
      stripes.add(stripeInfo);
      stripeStream.close();
      stripe = null;
      stripeStream = null;
      stripeStats = null;
      // finished writing temporary data file, close stream, we will reopen it for append
      temporaryStream.close();
      temporaryStream = null;
      LOG.debug("Finished writing temporary data file {}", tmpDataPath);

      LOG.debug("Merge stripe statistics");
      // combine all statistics for a file
      Statistics[] fileStats = createStatistics(td);
      for (StripeInformation info : stripes) {
        if (info.hasStatistics()) {
          for (int i = 0; i < fileStats.length; i++) {
            fileStats[i].merge(info.getStatistics()[i]);
          }
        }
      }

      // write complete file
      LOG.debug("Write file header");
      outputStream = fs.create(filePath, false, hdfsBufferSize);
      FileHeader fileHeader = new FileHeader(td, fileStats);
      fileHeader.setState(0, Riff.encodeCompressionCodec(codec));
      fileHeader.writeTo(outputStream);
      // write stripe information
      OutputBuffer buffer = new OutputBuffer();
      LOG.info("Write {} stripes", stripes.size());
      // when we save stripes they contain relative offset to the first stripe, when reading each
      // stripe, we would correct on current stream position.
      for (int i = 0; i < stripes.size(); i++) {
        stripes.get(i).writeExternal(buffer);
      }
      // flush buffer into output stream, close output stream similar to writing temporary data file
      // and report when it is done. In case of exceptions out will be closed in `finally` block
      buffer.align();
      LOG.info("Write metadata content of {} bytes", buffer.bytesWritten());
      outputStream.writeLong(((long) buffer.bytesWritten() << 32) + stripes.size());
      buffer.writeExternal(outputStream);
      // write bytes from temporary data path into final file; we use append stream to do it more
      // or less efficiently.
      long start = System.nanoTime();
      appendDataStream(outputStream);
      long end = System.nanoTime();
      LOG.debug("Append took {} ms", (end - start) / 1e6);
    } finally {
      // close temporary stream
      if (temporaryStream != null) {
        temporaryStream.close();
      }
      // close actual file stream
      if (outputStream != null) {
        outputStream.close();
      }
      // remove temporary data file
      fs.delete(tmpDataPath, false);
      // release codec resources
      if (codec != null) {
        codec.close();
      }
    }
    LOG.info("Finished writing file {}", filePath);
    writeFinished = true;
  }

  /**
   * Move content of temporary data file into destination stream. Append happens back-to-back, no
   * intermediate bytes are inserted, no compression is applied. Target stream is fully transferred.
   * Output stream is not closed by the end of this operation or in case of error.
   * @param out stream to copy into
   * @throws IOException
   */
  private void appendDataStream(FSDataOutputStream out) throws IOException {
    // we are going to use the same buffer size that was used for outstream
    FSAppendStream append = new FSAppendStream(out, bufferSize);
    FSDataInputStream in = fs.open(tmpDataPath, hdfsBufferSize);
    try {
      append.writeStream(in);
      // do not flush stream, we will do it when closing both temporary and output streams
    } catch (IOException ioe) {
      if (out != null) {
        out.close();
      }
      throw ioe;
    } finally {
      if (in != null) {
        in.close();
      }
    }
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

  /**
   * Create new array of column filters for a stripe.
   * Returns null if column filters are disabled.
   * @param td type description
   * @param enabled true if column filters are enabled
   * @param stripeRows expected number of rows in stripe
   * @return array of column filters or null if filters are disabled
   */
  private static ColumnFilter[] createColumnFilters(
      TypeDescription td, boolean enabled, int stripeRows) {
    // column filters are created only if enabled and type description contains any indexed fields
    if (!enabled || td.indexFields().length == 0) return null;
    ColumnFilter[] filters = new ColumnFilter[td.fields().length];
    for (int i = 0; i < td.fields().length; i++) {
      if (td.fields()[i].isIndexed()) {
        filters[i] = ColumnFilter.sqlTypeToColumnFilter(td.fields()[i].dataType(), stripeRows);
      } else {
        filters[i] = ColumnFilter.noopFilter();
      }
    }
    return filters;
  }

  /**
   * Update each instance of column filters with value of internal row.
   * @param filters array of filters, can be null
   * @param td type description
   * @param row row to use for updates
   */
  private static void updateColumnFilters(
      ColumnFilter[] filters, TypeDescription td, InternalRow row) {
    if (filters == null) return;
    for (int i = 0; i < filters.length; i++) {
      filters[i].update(row, td.atPosition(i).origSQLPos());
    }
  }

  @Override
  public String toString() {
    return "FileWriter[" +
      "path=" + filePath +
      ", type_desc=" + td +
      ", rows_per_stripe=" + numRowsInStripe +
      ", is_compressed=" + (codec != null) +
      ", buffer_size=" + bufferSize +
      ", hdfs_buffer_size=" + hdfsBufferSize +
      ", column_filter_enabled=" + columnFilterEnabled + "]";
  }
}
