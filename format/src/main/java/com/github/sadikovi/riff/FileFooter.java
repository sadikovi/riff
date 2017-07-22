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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.stats.Statistics;

/**
 * Footer for Riff file, stores dynamic information and written at the end of the file.
 */
public class FileFooter {
  private static final Logger LOG = LoggerFactory.getLogger(FileFooter.class);

  // file statistics
  private final Statistics[] fileStats;
  // number of records in file
  private final long numRecords;
  // byte buffer to load stripe information lazily
  private ByteBuffer buffer;
  // stripe information is dynamically loaded as required
  private StripeInformation[] stripes;

  /**
   * Initialize file footer with file statisitcs, number of records and stripe information.
   * If stripe information is null, it should be loaded from byte buffer, otherwise return it
   * directly. Byte buffer can be null, only if stripes have not been loaded.
   * @param fileStats file statistics
   * @param numRecords number of records in file
   * @param stripes list of stripe information
   * @param buffer byte buffer to load stripe information from
   */
  public FileFooter(
      Statistics[] fileStats, long numRecords, StripeInformation[] stripes, ByteBuffer buffer) {
    if (numRecords < 0) {
      throw new IllegalArgumentException("Negative number of records: " + numRecords);
    }
    this.fileStats = fileStats;
    this.numRecords = numRecords;
    this.stripes = stripes;
    this.buffer = buffer;
  }

  public FileFooter(Statistics[] fileStats, long numRecords, StripeInformation[] stripes) {
    this(fileStats, numRecords, stripes, null);
  }

  public FileFooter(Statistics[] fileStats, long numRecords, ByteBuffer buffer) {
    this(fileStats, numRecords, null, buffer);
  }

  /**
   * Create file footer from list of stripe information.
   * @param fileStats file statistics
   * @param numRecords number of records
   * @param stripes list of stripe info
   * @return file footer
   */
  public static FileFooter create(
      Statistics[] fileStats, long numRecords, List<StripeInformation> stripes) {
    StripeInformation[] arr = new StripeInformation[stripes.size()];
    int i = 0;
    while (i < arr.length) {
      arr[i] = stripes.get(i);
      ++i;
    }
    return new FileFooter(fileStats, numRecords, arr, null);
  }

  /**
   * Get file statistics.
   * @return list of file statistics
   */
  public Statistics[] getFileStatistics() {
    return fileStats;
  }

  /** Prepare stripes from byte buffer, if applicable */
  private void prepareStripeInformation() {
    if (stripes != null) return;
    if (buffer == null) {
      throw new IllegalArgumentException(
        "Byte buffer is null, cannot reconstruct stripe information");
    }
    int stripeLen = buffer.getInt(), i = 0;
    stripes = new StripeInformation[stripeLen];
    while (i < stripeLen) {
      try {
        stripes[i++] = StripeInformation.readExternal(buffer);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to read stripe " + i, ioe);
      }
    }
    buffer = null;
  }

  /**
   * Get stripe information in footer.
   * Loads stripes if they have not been materialized.
   * @return stripe information
   */
  public StripeInformation[] getStripeInformation() {
    prepareStripeInformation();
    return stripes;
  }

  /**
   * Get number of records recorded in file footer.
   * @return number of records in file
   */
  public long getNumRecords() {
    return numRecords;
  }

  /**
   * Write footer into output stream.
   * Stream is not closed after this operation is complete.
   * @param out output stream
   * @throws IOException
   */
  public void writeTo(FSDataOutputStream out) throws IOException {
    OutputBuffer buffer = new OutputBuffer();
    // write file metadata and number of records in file
    buffer.writeLong(numRecords);
    buffer.writeInt(fileStats.length);
    int i = 0;
    while (i < fileStats.length) {
      LOG.debug("Write file statistics {}", fileStats[i]);
      fileStats[i++].writeExternal(buffer);
    }
    // when we save stripes they contain relative offset to the first stripe, when reading each
    // stripe, we would correct on current stream position.
    prepareStripeInformation();
    LOG.debug("Write {} stripes", stripes.length);
    buffer.writeInt(stripes.length);
    i = 0;
    while (i < stripes.length) {
      stripes[i++].writeExternal(buffer);
    }
    // align first
    buffer.align();
    // magic and number of bytes to read should be written the last in the stream
    buffer.writeLong(((long) Riff.MAGIC << 32) + buffer.bytesWritten());
    LOG.debug("Write footer content of {} bytes", buffer.bytesWritten());
    // write buffer data
    buffer.writeExternal(out);
  }

  /**
   * Read footer from output stream.
   * Footer is assumed to be placed at the end of the stream. Seek is performed inside the method.
   * Stream is not closed after operation is complete.
   * @param in input stream
   * @param maxSize maximum stream size
   * @throws IOException
   */
  public static FileFooter readFrom(FSDataInputStream in, long maxSize) throws IOException {
    int tailOffset = 8;
    // stream size must be larger than magic + length
    if (maxSize < tailOffset) {
      throw new IOException("Invalid stream, cannot read footer: " + maxSize + " < " + tailOffset);
    }
    // Read 8 bytes: magic 4 bytes and length of the header 4 bytes
    ByteBuffer buffer = ByteBuffer.allocate(tailOffset);
    in.readFully(maxSize - tailOffset, buffer.array(), buffer.arrayOffset(), tailOffset);

    // reconstruct magic and written bytes
    long meta = buffer.getLong();
    int magic = (int) (meta >>> 32);
    if (magic != Riff.MAGIC) throw new IOException("Wrong magic: " + magic + " != " + Riff.MAGIC);
    int len = (int) (meta & 0x7fffffff);
    LOG.debug("Read footer content of {} bytes", len);

    // read full footer bytes
    buffer = ByteBuffer.allocate(len);
    in.readFully(maxSize - tailOffset - len, buffer.array(), buffer.arrayOffset(), len);
    // no flip - we have not reset position
    long numRecords = buffer.getLong();
    // read file statistics
    Statistics[] fileStats = new Statistics[buffer.getInt()];
    int i = 0;
    while (i < fileStats.length) {
      fileStats[i] = Statistics.readExternal(buffer);
      LOG.debug("Read file statistics {}", fileStats[i]);
      ++i;
    }
    return new FileFooter(fileStats, numRecords, buffer);
  }
}
