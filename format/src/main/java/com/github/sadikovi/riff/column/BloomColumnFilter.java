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

package com.github.sadikovi.riff.column;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.util.sketch.BloomFilter;

import com.github.sadikovi.riff.io.ByteBufferStream;
import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * [[BloomColumnFilter]] is backed by `BloomFilter` implementation in Spark sketch module.
 * Since it can take a fair amount of bytes to serialize, should be used for index fields only.
 * Each subclass must overwrite method to update filter for a given data type.
 */
abstract class BloomColumnFilter extends ColumnFilter {
  // default false positive probability
  private static final double DEFAULT_FPP = 0.5;
  // default number of records
  private static final long DEFAULT_EXPECTED_ITEMS = 16;
  // maximum number of items in filter
  private static final long MAX_EXPECTED_ITEMS = 100000;

  protected BloomFilter filter;

  BloomColumnFilter() {
    this(DEFAULT_EXPECTED_ITEMS, DEFAULT_FPP);
  }

  BloomColumnFilter(long numItems) {
    this(numItems, DEFAULT_FPP);
  }

  BloomColumnFilter(long numItems, double fpp) {
    this.filter = BloomFilter.create(Math.min(numItems, MAX_EXPECTED_ITEMS), fpp);
  }

  @Override
  protected void writeState(OutputBuffer buffer) throws IOException {
    // we have to create separate buffer to write bytes so we can read from byte buffer
    OutputBuffer tmp = new OutputBuffer();
    filter.writeTo(tmp);
    buffer.writeBytes(tmp.array());
  }

  @Override
  protected void readState(ByteBuffer buffer) throws IOException {
    // buffer is updated by stream
    ByteBufferStream in = new ByteBufferStream(buffer);
    filter = BloomFilter.readFrom(in);
  }
}
