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
  // path to the riff file
  private final Path filePath;
  // buffer size for instream
  private final int bufferSize;
  // HDFS buffer size for opening stream
  private final int hdfsBufferSize;
  // file header
  private FileHeader fileHeader;
  // whether or not read has been prepared, this flag is also set when reading type description
  private boolean readPrepared;

  FileReader(FileSystem fs, Configuration conf, Path path) {
    this.fs = fs;
    // we do not check if files exist, it will be checked in prepareRead method
    this.filePath = fs.makeQualified(path);
    this.bufferSize = Riff.Options.power2BufferSize(conf);
    this.hdfsBufferSize = Riff.Options.hdfsBufferSize(conf);
    // file header is only available after preparing read
    this.fileHeader = null;
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
      in = fs.open(filePath, hdfsBufferSize);
      // read input stream and return file state
      fileHeader = FileHeader.readFrom(in);
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
          skipFile = !state.indexTree().evaluateState(fileHeader.getFileStatistics());
        } else {
          skipFile = !state.tree().evaluateState(fileHeader.getFileStatistics());
        }
      }
      if (skipFile) {
        LOG.debug("Skip file {}", filePath);
        return Buffers.emptyRowBuffer(in);
      }
      // read stripe information until no bytes are available in buffer
      long meta = in.readLong();
      final int contentLen = (int) (meta >>> 32);
      final int stripeLen = (int) (meta & 0x7fffffff);
      LOG.debug("Read content of {} bytes", contentLen);
      LOG.debug("Found {} stripes in the file", stripeLen);
      ByteBuffer buffer = ByteBuffer.allocate(contentLen);
      // do not flip buffer after this operation as we write directly into underlying array
      in.readFully(buffer.array(), buffer.arrayOffset(), buffer.limit());
      StripeInformation[] stripes = new StripeInformation[stripeLen];
      for (int i = 0; i < stripes.length; i++) {
        stripes[i] = StripeInformation.readExternal(buffer);
      }
      // reevaluate stripes based on predicate tree
      stripes = evaluateStripes(stripes, state);
      LOG.debug("Prepare to read {} stripes", stripes.length);
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
   * Only available after calling prepareRead() or `readFileHeader` methods, since it file header
   * as part of that call, otherwise exception is thrown.
   * @return file header
   * @throws IllegalStateException if not set
   */
  public FileHeader getFileHeader() {
    if (fileHeader == null) {
      throw new IllegalStateException("File header is not set, did you call `prepareRead()` " +
        "or `readFileHeader` methods?");
    }
    return fileHeader;
  }

  /**
   * Read file header that is cached by this reader for subsequent requests.
   * This method should be invoked separately from `prepareRead()`, and after this call type
   * header is available with `getFileHeader()` call.
   * @return file header
   * @throws FileNotFoundException if header file does not exist
   * @throws IOException if IO error occurs
   */
  public FileHeader readFileHeader() throws FileNotFoundException, IOException {
    if (readPrepared) throw new IOException("Reader reuse");
    FSDataInputStream in = null;
    try {
      in = fs.open(filePath, hdfsBufferSize);
      fileHeader = FileHeader.readFrom(in);
      readPrepared = true;
      return fileHeader;
    } finally {
      if (in != null) {
        in.close();
      }
    }
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

  /**
   * Get file path for this reader.
   * Path might not exist on file system.
   * @return file path
   */
  public Path filePath() {
    return filePath;
  }

  /**
   * Return buffer size selected for this reader to use in instream.
   * @return buffer size
   */
  public int bufferSize() {
    return bufferSize;
  }

  @Override
  public String toString() {
    return "FileReader[" +
      "path=" + filePath +
      ", buffer_size=" + bufferSize +
      ", hdfs_buffer_size=" + hdfsBufferSize + "]";
  }
}
