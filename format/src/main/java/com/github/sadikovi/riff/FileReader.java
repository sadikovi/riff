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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.tree.Tree;

/**
 * File reader provides methods to read a Riff file and returns a row buffer that can be used as
 * iterator to traverse all available rows. Compression is inferred from the file, filter expression
 * can be passed to file reader to return only subset of rows. Class should be used per thread, is
 * not thread-safe.
 *
 * Usage:
 * {{{
 * FileReader reader = ...
 * // can have optional filter
 * RowBuffer rowbuf = reader.prepareRead();
 * while (rowbuf.hasNext()) {
 *   // process row
 *   process(rowbuf.next());
 * }
 * rowbuf.close();
 * }}}
 *
 * Reader should be created only one per file, no reuse is allowed.
 */
public class FileReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

  // file system to use
  private final FileSystem fs;
  // file status of the riff file
  private final FileStatus fileStatus;
  // buffer size for instream
  private final int bufferSize;
  // HDFS buffer size for opening stream
  private final int hdfsBufferSize;
  // file header
  private FileHeader fileHeader;
  // file footer
  private FileFooter fileFooter;
  // whether or not read has been prepared, this flag is also set when reading type description
  private boolean readPrepared;

  FileReader(FileSystem fs, Configuration conf, Path path) throws IOException {
    this(fs, conf, fs.getFileStatus(path));
  }

  FileReader(FileSystem fs, Configuration conf, FileStatus status) {
    this.fs = fs;
    this.fileStatus = status;
    this.bufferSize = Riff.Options.power2BufferSize(conf);
    this.hdfsBufferSize = Riff.Options.hdfsBufferSize(conf);
    // file header is only available after preparing read
    this.fileHeader = null;
    // file footer is only available after preparing read
    this.fileFooter = null;
    this.readPrepared = false;
  }

  /**
   * Prepare row buffer without filter.
   * @return row buffer
   * @throws IOException
   */
  public RowBuffer prepareRead() throws IOException {
    return prepareRead(null);
  }

  /**
   * Prepare row buffer based on file path.
   * @param filter optional filter, if null then no filter applied
   * @return row buffer
   * @throws FileNotFoundException if either data or header file is not found
   * @throws IOException
   */
  public RowBuffer prepareRead(Tree filter) throws FileNotFoundException, IOException {
    if (readPrepared) throw new IOException("Reader reuse");
    // we start with reading file header and extracting all information that is required to
    // validate file and/or resolve statistics
    FSDataInputStream in = null;
    try {
      in = fs.open(fileStatus.getPath(), hdfsBufferSize);
      // read input stream and return file state
      fileHeader = FileHeader.readFrom(in);
      fileFooter = FileFooter.readFrom(in, fileStatus.getLen());
      LOG.debug("Found type description {}", fileHeader.getTypeDescription());
      CompressionCodec codec = Riff.decodeCompressionCodec(fileHeader.state(0));
      if (codec == null) {
        LOG.debug("Found no compression codec, using uncompressed");
      } else {
        LOG.debug("Found compression codec {}", codec);
      }
      // initialize valid predicate state if necessary
      PredicateState state = null;
      if (filter != null) {
        state = new PredicateState(filter, fileHeader.getTypeDescription());
      }
      // if predicate state is available - evaluate tree and decide on whether or not to read the
      // file any further.
      boolean skipFile = false;
      if (state != null) {
        if (state.hasIndexedTreeOnly()) {
          skipFile = !state.indexTree().evaluateState(fileFooter.getFileStatistics());
        } else {
          skipFile = !state.tree().evaluateState(fileFooter.getFileStatistics());
        }
      }
      if (skipFile) {
        LOG.debug("Skip file {}", fileStatus.getPath());
        return Buffers.emptyRowBuffer(in);
      }
      // reevaluate stripes based on predicate tree
      StripeInformation[] stripes = fileFooter.getStripeInformation();
      stripes = evaluateStripes(stripes, state);
      LOG.debug("Prepare iterator to read data from {} stripes", stripes.length);
      readPrepared = true;
      return Buffers.prepareRowBuffer(in, stripes, fileHeader.getTypeDescription(), codec,
        bufferSize, state);
    } catch (IOException ioe) {
      if (in != null) {
        in.close();
      }
      throw ioe;
    }
  }

  /**
   * File header information for this reader.
   * Only available after calling prepareRead() or `readFileHeader` methods, because it reads file
   * header as part of that call, otherwise exception is thrown.
   * @return file header
   * @throws IllegalStateException if not set
   */
  public FileHeader getFileHeader() {
    if (fileHeader == null) {
      throw new IllegalStateException("File header is not set, did you call `prepareRead()` " +
        "or `readFileInfo(false/true)` methods?");
    }
    return fileHeader;
  }

  /**
   * File footer for this reader.
   * Only available after calling prepareRead() or `readFileFooter` methods.
   * @return file footer
   * @throws IllegalStateException if not set
   */
  public FileFooter getFileFooter() {
    if (fileFooter == null) {
      throw new IllegalStateException("File footer is not set, did you call `prepareRead()` " +
        "or `readFileInfo(true)` methods?");
    }
    return fileFooter;
  }

  /**
   * Read file header and footer that is cached by this reader for subsequent requests.
   * This method should be invoked separately from `prepareRead()`, and after this call type
   * header is available with `getFileHeader()` and footer is available with `getFileFooter()`.
   * @param readFooter when set to true, reads footer as well, otherwise only header
   * @throws FileNotFoundException if file does not exist
   * @throws IOException if IO error occurs
   */
  public void readFileInfo(boolean readFooter) throws FileNotFoundException, IOException {
    if (readPrepared) throw new IOException("Reader reuse");
    FSDataInputStream in = null;
    try {
      in = fs.open(fileStatus.getPath(), hdfsBufferSize);
      fileHeader = FileHeader.readFrom(in);
      if (readFooter) {
        fileFooter = FileFooter.readFrom(in, fileStatus.getLen());
      }
      readPrepared = true;
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  /**
   * Get file property.
   * Header must be initialized before calling this method.
   * @param key file property key
   * @return value as String or null, if no such key exists
   */
  public String getFileProperty(String key) {
    return getFileHeader().getProperty(key);
  }

  /**
   * Get file path for this reader.
   * @return file path
   */
  public Path filePath() {
    return getFileStatus().getPath();
  }

  /**
   * Get file status for this reader.
   * @return file status
   */
  public FileStatus getFileStatus() {
    return fileStatus;
  }

  /**
   * Return buffer size selected for this reader to use in instream.
   * @return buffer size
   */
  public int bufferSize() {
    return bufferSize;
  }

  /**
   * Evaluate and reduce stripes based on state. Returned stripes array will have at most all
   * elements of the original stripes. All stripes are sorted in ascending order based on offset.
   * @param stripes original array of stripes
   * @param state predicate state to evaluate, can be null
   * @return reduced stripe information
   */
  protected static StripeInformation[] evaluateStripes(
      StripeInformation[] stripes,
      PredicateState state) {
    if (state != null) {
      // if stripe has statistics it is evaluated against predicate state, otherwise it is always
      // included in final result
      int stripesLeft = stripes.length;
      for (int i = 0; i < stripes.length; i++) {
        boolean keep = true;
        if (stripes[i].hasStatistics()) {
          if (state.hasIndexedTreeOnly()) {
            keep = state.indexTree().evaluateState(stripes[i].getStatistics());
          } else {
            keep = state.tree().evaluateState(stripes[i].getStatistics());
          }
        }
        // if predicate passes statistics, evaluate column filters
        if (keep && stripes[i].hasColumnFilters()) {
          if (state.hasIndexedTreeOnly()) {
            keep = state.indexTree().evaluateState(stripes[i].getColumnFilters());
          } else {
            keep = state.tree().evaluateState(stripes[i].getColumnFilters());
          }
        }

        if (!keep) {
          stripes[i] = null;
          stripesLeft--;
        }
      }

      if (stripesLeft < stripes.length) {
        StripeInformation[] reducedStripes = new StripeInformation[stripesLeft];
        // write stripes preserving original order
        for (int i = stripes.length - 1; i >= 0; i--) {
          if (stripes[i] != null) {
            reducedStripes[--stripesLeft] = stripes[i];
          }
        }
        stripes = reducedStripes;
      }
    }
    Arrays.sort(stripes);
    return stripes;
  }

  @Override
  public String toString() {
    return "FileReader[" +
      "status=" + fileStatus +
      ", buffer_size=" + bufferSize +
      ", hdfs_buffer_size=" + hdfsBufferSize + "]";
  }
}
