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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.spark.sql.catalyst.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.InStream;
import com.github.sadikovi.riff.io.StripeInputBuffer;

/**
 * Abstract class for reading internal rows from provided stream.
 */
public class RowBuffer implements Iterator<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(RowBuffer.class);

  // raw input stream for buffer
  private FSDataInputStream in;
  // sorted by offsets stripes
  private StripeInformation[] stripes;
  // row reader
  private IndexedRowReader reader;
  // compression codec to use, can be null (no compression applied)
  private final CompressionCodec codec;
  // buffer size for intermediate instream
  private final int bufferSize;
  // index of the current stripe
  private int currentStripeIndex;
  // current stripe buffer
  private StripeInputBuffer currentStripe;
  // current buffered instream
  private InStream currentStream;

  /**
   * Create new row buffer.
   * `in` should be newly opened data input stream, and `stripes` array should be sorted already by
   * offset to minimize number of seeks/skips within stream. Buffer does not validate stripes on
   * statistics, therefore they should be filtered before passing into this method.
   * @param in raw input stream
   * @param stripes sorted array of stripes to read
   * @param td type description for rows to read
   * @param codec compression codec (null - no compression)
   * @param bufferSize buffer size for instream
   */
  protected RowBuffer(
      FSDataInputStream in,
      StripeInformation[] stripes,
      TypeDescription td,
      CompressionCodec codec,
      int bufferSize) {
    if (in == null) throw new IllegalArgumentException("Null input stream");
    if (stripes == null) throw new IllegalArgumentException("Null stripes list");
    if (bufferSize <= 0) throw new IllegalArgumentException("Invalid buffer size: " + bufferSize);
    this.in = in;
    this.stripes = stripes;
    this.reader = new IndexedRowReader(td);
    this.codec = codec;
    this.bufferSize = bufferSize;
    // set state to null
    this.currentStripeIndex = 0;
    this.currentStripe = null;
    this.currentStream = null;
  }

  /**
   * Method resets current stripe and instream to read the next batch of data.
   * If there are no stripes left, this method is no-op.
   * @throws IOException
   */
  private void bufferStripe() throws IOException {
    if (currentStripeIndex >= stripes.length) return;
    StripeInformation info = stripes[currentStripeIndex++];
    // reset state
    if (currentStream != null) {
      currentStream.close();
      currentStripe.close();
      currentStream = null;
      currentStripe = null;
    }
    LOG.info("Read stripe {}", info);
    // seek to a position in raw stream
    in.seek(info.offset());
    byte[] bytes = new byte[info.length()];
    in.readFully(bytes);
    currentStripe = new StripeInputBuffer(info.id(), bytes);
    currentStream = new InStream(bufferSize, codec, currentStripe);
    LOG.info("Buffer new stream {}", currentStream);
  }

  @Override
  public boolean hasNext() {
    try {
      // check if there are bytes in the stream or buffer next stripe
      if (currentStream != null && currentStream.available() != 0) {
        return true;
      }
      bufferStripe();
      return currentStream != null && currentStream.available() != 0;
    } catch (IOException ioe) {
      LOG.error("Failed to read stream={}, stripe input={}", currentStream, currentStripe);
      close();
      throw new RuntimeException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public InternalRow next() {
    if (!hasNext()) throw new NoSuchElementException("Empty iterator");
    try {
      return reader.readRow(currentStream);
    } catch (IOException ioe) {
      LOG.error("Failed to read stream={}, stripe input={}", currentStream, currentStripe);
      throw new RuntimeException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Close underlying stream and release resources associated with this row buffer.
   */
  public void close() {
    try {
      // release current stripe data
      if (currentStream != null) {
        currentStream.close();
        currentStripe.close();
        currentStream = null;
        currentStripe = null;
      }
      stripes = null;
      // release raw stream
      if (in != null) {
        in.close();
        in = null;
      }
    } catch (IOException ioe) {
      LOG.warn("Exception occuried during release of resources: {}", ioe.getMessage());
    }
  }
}
