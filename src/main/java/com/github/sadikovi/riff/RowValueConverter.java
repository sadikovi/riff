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

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Row value converter provies specialized method to write non-null value into output stream.
 */
public interface RowValueConverter {
  /**
   * Write value with either fixed or variable length into output buffer. Value is guaranteed to be
   * non-null and buffer is valid. Offset is length of fixed part, used for writing values with
   * variable part.
   */
  public void writeDirect(
      InternalRow row,
      int ordinal,
      OutputBuffer fixedBuffer,
      int fixedOffset,
      OutputBuffer variableBuffer) throws IOException;

  /**
   * Fixed offset in bytes for data type, this either includes value for primitive types, or fixed
   * sized metadata (either int or long) for non-primitive types, e.g. UTF8String.
   */
  public int byteOffset();
}
