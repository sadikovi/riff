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
import java.util.HashMap;
import java.util.Iterator;

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
import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.io.OutStream;
import com.github.sadikovi.riff.io.StripeOutputBuffer;
import com.github.sadikovi.riff.stats.Statistics;

/**
 * File writer provides methods to prepare file and write rows. File consists of three regions:
 * static metadata (FileHeader), main data in stripes, and dynamic metadata (FileFooter). Static
 * metadata includes type description and state, dynamic metadata includes statistics and stripe
 * information.
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
 * finalizes riff file.
 */
public class FileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

  // file system to use for writing a file
  private final FileSystem fs;
  // resolved path for final Riff file
  private final Path filePath;
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
  // file output stream
  private FSDataOutputStream outputStream;
  // custom file properties, written as part of header
  private HashMap<String, String> fileProperties;
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
  // total number of records in file
  private int totalRecords;
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
    this.writePrepared = false;
    this.writeFinished = false;
    if (this.fs.exists(filePath)) {
      throw new FileAlreadyExistsException("Already exists: " + filePath);
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
    // file properties, by default not initialized
    this.fileProperties = null;
  }

  /**
   * Return file path for writer.
   * @return qualified file path
   */
  public Path filePath() {
    return filePath;
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
   * Set custom file property to be written as part of file header.
   * Must be called before 'prepareWrite()' method.
   * @param key non-null key
   * @param value non-null value for the key
   */
  public void setFileProperty(String key, String value) {
    if (writePrepared || writeFinished) {
      throw new IllegalStateException("Cannot set property on already prepared/finished file. " +
        "Method 'setFileProperty()' should be called before 'prepareWrite()'");
    }
    if (key == null || value == null) {
      throw new IllegalArgumentException("Null key/value: key=" + key + ", value=" + value);
    }
    if (fileProperties == null) {
      fileProperties = new HashMap<String, String>();
    }
    fileProperties.put(key, value);
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
    totalRecords = 0;
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
    // create stream for riff file and write header information
    FileHeader fileHeader = new FileHeader(td, fileProperties);
    fileHeader.setState(0, Riff.encodeCompressionCodec(codec));
    try {
      outputStream = fs.create(filePath, false, hdfsBufferSize);
      LOG.debug("Write file header");
      fileHeader.writeTo(outputStream);
    } catch (IOException ioe) {
      if (outputStream != null) {
        outputStream.close();
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
        // written numRowsInStripe records
        totalRecords += numRowsInStripe;
        stripe.flush(outputStream);
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
      if (outputStream != null) {
        outputStream.close();
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
      stripe.flush(outputStream);
      LOG.debug("Finished writing stripe {}, records={}", stripeInfo,
        numRowsInStripe - stripeCurrentRecords);
      // update total records with delta
      totalRecords += numRowsInStripe - stripeCurrentRecords;
      stripes.add(stripeInfo);
      stripeStream.close();
      stripe = null;
      stripeStream = null;
      stripeStats = null;

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
      LOG.debug("Write file footer");
      FileFooter fileFooter = FileFooter.create(fileStats, totalRecords, stripes);
      fileFooter.writeTo(outputStream);
    } finally {
      // close file stream
      if (outputStream != null) {
        outputStream.close();
      }
      // release codec resources
      if (codec != null) {
        codec.close();
      }
    }
    LOG.info("Finished writing file {}", filePath);
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
