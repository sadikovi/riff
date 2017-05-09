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

package com.github.sadikovi.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object riff {
  // format fully-qualified path
  private val formatName = classOf[DefaultSource].getCanonicalName

  /**
   * Adds a shortcut method `riff()` to DataFrameReader that allows to omit specifying format.
   */
  implicit class RiffDataFrameReaderSupport(reader: DataFrameReader) {
    def riff: String => DataFrame = reader.format(formatName).load
  }

  /**
   * Adds a shortcut method `riff()` to DataFrameWriter that allows to omit specifying format.
   */
  implicit class RiffDataFrameWriterSupport[T](writer: DataFrameWriter[T]) {
    def riff: String => Unit = writer.format(formatName).save
  }
}
