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

import java.util.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * [[RowBuffer]] interface to return set of records from input stream.
 * See `Buffers` class for available implementations.
 */
public interface RowBuffer extends Iterator<InternalRow> {
  /**
   * Return true, if buffer has more records to read.
   * @return true if records are available, false otherwise
   */
  boolean hasNext();

  /**
   * Return next record from buffer.
   * @return internal row
   */
  InternalRow next();

  /**
   * Remove operation is currently not supported.
   */
  void remove();

  /**
   * Close current buffer and release all resources associated with this buffer.
   */
  void close();
}
