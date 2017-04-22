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

import com.github.sadikovi.riff.io.OutputBuffer;
import com.github.sadikovi.riff.io.StripeOutputBuffer;

/**
 * Interface for single stripe.
 * Contains information for reader.
 */
public class StripeInformation {
  public static final byte MAGIC = 47;

  private final short id;
  private final long offset;
  private final int length;
  // array of statistics, each index matches type spec index in type description
  private final Statistics[] stats;

  public StripeInformation(StripeOutputBuffer stripe, long pos) {
    this(stripe.id(), pos, stripe.length(), null);
  }

  public StripeInformation(StripeOutputBuffer stripe, long pos, Statistics[] stats) {
    this(stripe.id(), pos, stripe.length(), stats);
  }

  public StripeInformation(short id, long offset, int length, Statistics[] stats) {
    if (id < 0) throw new IllegalArgumentException("Negative id: " + id);
    if (offset < 0) throw new IllegalArgumentException("Negative offset: " + offset);
    if (length < 0) throw new IllegalArgumentException("Negative length: " + length);
    this.id = id;
    this.offset = offset;
    this.length = length;
    this.stats = stats;
  }

  /**
   * Get stripe id within a file.
   * @return stripe id
   */
  public short id() {
    return this.id;
  }

  /**
   * Get offset in bytes within a file.
   * @return stripe offset
   */
  public long offset() {
    return this.offset;
  }

  /**
   * Get stripe length in bytes.
   * @return bytes for stripe
   */
  public int length() {
    return this.length;
  }

  /**
   * Whether or not this stripe has column statistics.
   * @return true if stripe has statistics, false otherwise
   */
  public boolean hasStatistics() {
    return this.stats != null;
  }

  /**
   * Get statistics for this stripe, can return null - see `hasStatistics()` method.
   * Returned instance should be considered read-only.
   * @return stripe statistics
   */
  public Statistics[] getStatistics() {
    return this.stats;
  }

  /**
   * Write stripe information into external stream.
   * @param buffer output buffer
   * @throws IOException
   */
  public void writeExternal(OutputBuffer buffer) throws IOException {
    byte flags = 0;
    // flag per bit, e.g. stripe has statistics, etc.
    flags |= hasStatistics() ? 1 : 0;
    // stripe identifiers and flags
    buffer.writeByte(MAGIC);
    buffer.writeByte(flags);
    buffer.writeShort(id());
    // stripe stream information
    buffer.writeLong(offset());
    buffer.writeInt(length());
    // stripe statistics information
    if (hasStatistics()) {
      buffer.writeInt(stats.length);
      // statistics instance should never be null
      for (Statistics obj : stats) {
        if (obj == null) {
          throw new NullPointerException("Encountered null statistics for stripe " + this);
        }
        obj.writeExternal(buffer);
      }
    }
  }

  /**
   * Read stripe information from provided byte buffer.
   * @param buf byte buffer
   * @throws IOException
   */
  public static StripeInformation readExternal(ByteBuffer buf) throws IOException {
    // check magic
    int magic = buf.get();
    if (magic != MAGIC) {
      throw new IOException("Wrong magic: " + magic + " != " + MAGIC);
    }
    // stripe flags
    byte flags = buf.get();
    boolean hasStatistics = (flags & 1) != 0;
    short id = buf.getShort();
    long offset = buf.getLong();
    int length = buf.getInt();
    Statistics[] stats = null;
    if (hasStatistics) {
      int len = buf.getInt();
      stats = new Statistics[len];
      for (int i = 0; i < len; i++) {
        stats[i] = Statistics.readExternal(buf);
      }
    }
    return new StripeInformation(id, offset, length, stats);
  }

  @Override
  public String toString() {
    return "Stripe[id=" + id + ", offset=" + offset + ", length=" + length +
      ", has_stats=" + hasStatistics() + "]";
  }
}
