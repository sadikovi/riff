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
import com.github.sadikovi.riff.tree.TreeNode;

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
  // path to the header file
  private final Path headerPath;
  // path to the data file
  private final Path dataPath;
  // buffer size for instream
  private final int bufferSize;
  // HDFS buffer size for opening stream
  private final int hdfsBufferSize;

  FileReader(FileSystem fs, Configuration conf, Path path) {
    this.fs = fs;
    // we do not check if files exist, it will be checked in prepareRead method
    this.headerPath = fs.makeQualified(path);
    this.dataPath = Riff.makeDataPath(this.headerPath);
    this.bufferSize = Riff.Options.power2BufferSize(conf);
    this.hdfsBufferSize = Riff.Options.hdfsBufferSize(conf);
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
   * Prepare row buffer based on header and data paths.
   * @param filter optional filter, if null then no filter applied
   * @return row buffer
   * @throws FileNotFoundException if either data or header file is not found
   * @throws IOException
   */
  public RowBuffer prepareRead(TreeNode filter) throws FileNotFoundException, IOException {
    // we start with reading header file and extracting all information that is required to validate
    // file and/or resolve statistics
    FSDataInputStream in = null;
    try {
      in = fs.open(headerPath, hdfsBufferSize);
      // read input stream and return header file id, this will be used to compare with data file
      byte[] fileId = readHeader(in);
      // extract 8 byte long flags
      byte[] flags = readHeaderState(in);
      // read type description
      TypeDescription td = TypeDescription.readExternal(in);
      LOG.info("Found type description {}", td);
      LOG.info("Read header flags {}", Arrays.toString(flags));
      CompressionCodec codec = Riff.decodeCompressionCodec(flags[0]);
      if (codec == null) {
        LOG.info("Found no compression codec");
      } else {
        LOG.info("Found compression codec {}", codec);
      }
      // initialize valid predicate state if necessary
      PredicateState state = null;
      if (filter != null) {
        state = new PredicateState(filter, td);
      }

      // content of the header file is split into 2 parts:
      // 1. File statistics
      // 2. Stripe information for data file
      // To optimize reads we first load file statistics and resolve any filters provided. Depending
      // on the outcome of evaluation, we either proceed reading stripes or return empty row buffer

      // read file statistics content of the stream into byte buffer
      final int len = in.readInt();
      LOG.info("Read file statistics content of {} bytes", len);
      ByteBuffer buffer = ByteBuffer.allocate(len);
      in.read(buffer);
      buffer.flip();
      // read file statistics
      Statistics[] fileStats = new Statistics[buffer.getInt()];
      for (int i = 0; i < fileStats.length; i++) {
        fileStats[i] = Statistics.readExternal(buffer);
        LOG.info("Read file statistics {}", fileStats[i]);
      }
      // if predicate state is available - evaluate tree and decide on whether or not to read the
      // file any further.
      boolean skipFile = false;
      if (state != null) {
        if (state.hasIndexedTreeOnly()) {
          skipFile = !state.indexTree().evaluate(fileStats);
        } else {
          skipFile = !state.tree().evaluate(fileStats);
        }
      }
      if (skipFile) {
        LOG.info("Skip file {}", headerPath);
        in.close();
        return Buffers.emptyRowBuffer();
      }
      // read stripe information until no bytes are available in buffer
      final int contentLen = in.readInt();
      LOG.info("Read content of {} bytes", contentLen);
      buffer = ByteBuffer.allocate(contentLen);
      in.read(buffer);
      buffer.flip();
      StripeInformation[] stripes = new StripeInformation[buffer.getInt()];
      for (int i = 0; i < stripes.length; i++) {
        stripes[i] = StripeInformation.readExternal(buffer);
      }
      in.close();
      // reevaluate stripes based on predicate tree
      stripes = evaluateStripes(stripes, state);
      // open data file and check file id
      in = fs.open(dataPath, hdfsBufferSize);
      assertBytes(fileId, readHeader(in), "Wrong file id");
      return Buffers.prepareRowBuffer(in, stripes, td, codec, bufferSize, state);
    } catch (IOException ioe) {
      if (in != null) {
        in.close();
      }
      throw ioe;
    }
  }

  /**
   * Read header and return byte array of file id.
   * @param in input stream
   * @return file id
   * @throws IOException
   */
  private static byte[] readHeader(FSDataInputStream in) throws IOException {
    // in total we read 16 bytes of header, this includes 4 bytes of magic and 12 bytes of file id
    try {
      byte[] magic1 = Riff.MAGIC.getBytes();
      byte[] magic2 = new byte[4];
      in.readFully(magic2);
      assertBytes(magic1, magic2, "Wrong magic");
      // read file id
      byte[] fileId = new byte[12];
      in.readFully(fileId);
      return fileId;
    } catch (IOException ioe) {
      throw new IOException("Could not read header bytes", ioe);
    }
  }

  /**
   * Read header state and return encoded flags.
   * @param in input stream
   * @return byte array of flags (8 bytes long)
   * @throws IOException
   */
  private static byte[] readHeaderState(FSDataInputStream in) throws IOException {
    // header state is 8 bytes and only exists in header file
    try {
      byte[] flags = new byte[8];
      in.readFully(flags);
      return flags;
    } catch (IOException ioe) {
      throw new IOException("Could not read header state bytes", ioe);
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
            keep = state.indexTree().evaluate(stripes[i].getStatistics());
          } else {
            keep = state.tree().evaluate(stripes[i].getStatistics());
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
   * Assert file id based on provided expected file id.
   * @param fileId found file id
   * @param expectedFileId expected file id
   * @throws AssertionError if file ids do not match
   */
  protected static void assertBytes(byte[] arr1, byte[] arr2, String prefix) {
    String msg = prefix + ": " + ((arr1 == null) ? "null" : Arrays.toString(arr1)) + " != " +
      ((arr2 == null) ? "null" : Arrays.toString(arr2));
    if (arr1 == null || arr2 == null || arr1.length != arr2.length) {
      throw new AssertionError(msg);
    }
    for (int i = 0; i < arr1.length; i++) {
      if (arr1[i] != arr2[i]) {
        throw new AssertionError(msg);
      }
    }
  }

  /**
   * Get header path for this reader.
   * Path might not exist on file system.
   * @return header path
   */
  public Path headerPath() {
    return headerPath;
  }

  /**
   * Get data path for this reader.
   * Path might not exist on file system.
   * @return data path
   */
  public Path dataPath() {
    return dataPath;
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
      "header=" + headerPath +
      ", data=" + dataPath +
      ", buffer_size=" + bufferSize +
      ", hdfs_buffer_size=" + hdfsBufferSize + "]";
  }
}
