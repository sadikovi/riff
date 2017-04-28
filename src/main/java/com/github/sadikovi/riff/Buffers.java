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
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.spark.sql.catalyst.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.CompressionCodec;
import com.github.sadikovi.riff.io.InStream;
import com.github.sadikovi.riff.io.StripeInputBuffer;

/**
 * Container for available row buffers.
 * Use `prepareRowBuffer()` to find buffer that matches input parameters.
 */
public class Buffers {
  private static final Logger LOG = LoggerFactory.getLogger(RowBuffer.class);

  private Buffers() { }

  /**
   * Select row buffer based on provided options.
   * @param in raw input stream
   * @param stripes stripe information, should be sorted by offsets in ascending order
   * @param td type description for records
   * @param codec compression codec, null - no compression
   * @param bufferSize buffer size for instream
   * @param state predicate state to use, null - no predicate, direct scan
   * @return row buffer based on input parameters
   */
  public static RowBuffer prepareRowBuffer(
      FSDataInputStream in,
      StripeInformation[] stripes,
      TypeDescription td,
      CompressionCodec codec,
      int bufferSize,
      PredicateState state) {
    RowBuffer rowbuf = null;
    if (stripes == null || stripes.length == 0) {
      rowbuf = new EmptyRowBuffer(in);
    } else {
      // resolve state: if state is negative trivial return empty buffer, otherwise choose
      // depending on availability of state
      if (state != null && !state.isResultKnown()) {
        rowbuf = new PredicateScanRowBuffer(in, stripes, td, codec, bufferSize, state);
      } else if (state == null) {
        rowbuf = new DirectScanRowBuffer(in, stripes, td, codec, bufferSize);
      } else {
        // at this point state is known to contain trivial result
        LOG.debug("Analyze state {}", state);
        if (state.result()) {
          rowbuf = new DirectScanRowBuffer(in, stripes, td, codec, bufferSize);
        } else {
          rowbuf = new EmptyRowBuffer(in);
        }
      }
    }
    LOG.info("Select row buffer {}", rowbuf);
    return rowbuf;
  }

  /**
   * Empty row buffer.
   * This row buffer represents empty iterator and is created when no stripes are available for
   * read or predicate state is either undefined or trivial
   */
  static class EmptyRowBuffer implements RowBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(RowBuffer.class);

    // raw input stream
    private FSDataInputStream in;

    protected EmptyRowBuffer(FSDataInputStream in) {
      this.in = in;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public InternalRow next() {
      throw new NoSuchElementException("Empty iterator");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Exception occuried during release of resources: {}", ioe.getMessage());
      } finally {
        in = null;
      }
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  /**
   * Abstract class for reading internal rows from provided stream.
   * Extends row buffer interface with default methods, subclasses overwrite specific
   * implementation details.
   */
  static abstract class InternalRowBuffer implements RowBuffer {
    protected static final Logger LOG = LoggerFactory.getLogger(RowBuffer.class);

    // raw input stream for buffer
    private FSDataInputStream in;
    // sorted by offsets stripes
    private StripeInformation[] stripes;
    // compression codec to use, can be null (no compression applied)
    private final CompressionCodec codec;
    // buffer size for intermediate instream
    private final int bufferSize;
    // current stripe information
    private StripeInformation info;
    // index of the current stripe
    private int currentStripeIndex;
    // current stripe buffer
    protected StripeInputBuffer currentStripe;
    // current buffered instream
    protected InStream currentStream;

    /**
     * Create new row buffer.
     * `in` should be newly opened data input stream, and `stripes` array should be sorted already by
     * offset to minimize number of seeks/skips within stream. Buffer does not validate stripes on
     * statistics, therefore they should be filtered before passing into this method.
     * @param in raw input stream
     * @param stripes sorted array of stripes to read
     * @param codec compression codec (null - no compression)
     * @param bufferSize buffer size for instream
     */
    protected InternalRowBuffer(
        FSDataInputStream in,
        StripeInformation[] stripes,
        CompressionCodec codec,
        int bufferSize) {
      if (in == null) throw new IllegalArgumentException("Null input stream");
      if (stripes == null) throw new IllegalArgumentException("Null stripes list");
      if (bufferSize <= 0) throw new IllegalArgumentException("Invalid buffer size: " + bufferSize);
      this.in = in;
      this.stripes = stripes;
      this.codec = codec;
      this.bufferSize = bufferSize;
      // set state to null
      this.info = null;
      this.currentStripeIndex = 0;
      this.currentStripe = null;
      this.currentStream = null;
    }

    /**
     * Method resets current stripe and instream to read the next batch of data.
     * If there are no stripes left, this method is no-op.
     * @throws IOException
     */
    protected void bufferStripe() throws IOException {
      if (currentStripeIndex >= stripes.length) return;
      info = stripes[currentStripeIndex++];
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
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Close underlying stream and release resources associated with this row buffer.
     */
    @Override
    public void close() {
      try {
        // release current stripe data
        if (currentStream != null) {
          currentStream.close();
          currentStripe.close();
          currentStream = null;
          currentStripe = null;
        }
        info = null;
        stripes = null;
        // release raw stream
        if (in != null) {
          in.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Exception occuried during release of resources: {}", ioe.getMessage());
      } finally {
        in = null;
      }
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  /**
   * Direct scan row buffer.
   * Does not filter records either based on stripe statistics or per record. Reader
   * iniatialization is default.
   */
  static class DirectScanRowBuffer extends InternalRowBuffer {
    // indexed row reader
    private final IndexedRowReader reader;

    protected DirectScanRowBuffer(
        FSDataInputStream in,
        StripeInformation[] stripes,
        TypeDescription td,
        CompressionCodec codec,
        int bufferSize) {
      super(in, stripes, codec, bufferSize);
      this.reader = new IndexedRowReader(td);
      LOG.info("Created reader {}", reader);
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
  }

  /**
   * Predicate scan row buffer.
   * Evaluates predicate state to each record, will buffer stripe until next record is found.
   * Does not perform filtering on stripe statistics.
   */
  static class PredicateScanRowBuffer extends InternalRowBuffer {
    private boolean found;
    private InternalRow currentRow;
    private final IndexedRowReader reader;
    private final PredicateState state;

    protected PredicateScanRowBuffer(
        FSDataInputStream in,
        StripeInformation[] stripes,
        TypeDescription td,
        CompressionCodec codec,
        int bufferSize,
        PredicateState state) {
      super(in, stripes, codec, bufferSize);
      this.reader = new IndexedRowReader(td);
      LOG.info("Created reader {}", reader);
      this.state = state;
      this.found = false;
      this.currentRow = null;
    }

    @Override
    public boolean hasNext() {
      try {
        while (!found) {
          // check if there are bytes in the stream or buffer next stripe
          if (currentStream == null || currentStream.available() <= 0) {
            bufferStripe();
          }
          // if stream is still empty after buffering we break loop
          if (currentStream == null || currentStream.available() <= 0) {
            break;
          }
          currentRow = reader.readRow(currentStream, state);
          if (currentRow != null) {
            found = true;
          }
        }
        return found;
      } catch (IOException ioe) {
        LOG.error("Failed to read stream={}, stripe input={}", currentStream, currentStripe);
        close();
        throw new RuntimeException(ioe.getMessage(), ioe);
      }
    }

    @Override
    public InternalRow next() {
      if (!found) throw new NoSuchElementException("Empty iterator");
      // this should never happen when operating correctly, since `found` would be false
      if (currentRow == null) throw new IllegalStateException("Out of sync in " + this);
      found = false;
      return currentRow;
    }
  }
}
