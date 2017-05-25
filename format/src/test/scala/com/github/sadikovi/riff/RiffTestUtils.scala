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

package com.github.sadikovi.riff

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.column.ColumnFilter
import com.github.sadikovi.riff.stats.Statistics

/**
 * General utility methods for unittests.
 * Mainly added for convenience of creating different objects and shortcuts.
 */
object RiffTestUtils {
  def stats(min: Int, max: Int, nulls: Boolean) = {
    val stats = Statistics.sqlTypeToStatistics(IntegerType)
    if (nulls) stats.update(InternalRow(null), 0)
    stats.update(InternalRow(min), 0)
    stats.update(InternalRow(max), 0)
    stats
  }

  def stats(min: Long, max: Long, nulls: Boolean) = {
    val stats = Statistics.sqlTypeToStatistics(LongType)
    if (nulls) stats.update(InternalRow(null), 0)
    stats.update(InternalRow(min), 0)
    stats.update(InternalRow(max), 0)
    stats
  }

  def stats(min: String, max: String, nulls: Boolean) = {
    val stats = Statistics.sqlTypeToStatistics(StringType)
    if (nulls) stats.update(InternalRow(null), 0)
    stats.update(InternalRow(UTF8String.fromString(min)), 0)
    stats.update(InternalRow(UTF8String.fromString(max)), 0)
    stats
  }

  def filter(value: Int): ColumnFilter = {
    val filter = ColumnFilter.sqlTypeToColumnFilter(IntegerType, 10)
    filter.update(InternalRow(value), 0)
    filter
  }

  def filter(value: Long): ColumnFilter = {
    val filter = ColumnFilter.sqlTypeToColumnFilter(LongType, 10)
    filter.update(InternalRow(value), 0)
    filter
  }

  def filter(value: String): ColumnFilter = {
    val filter = ColumnFilter.sqlTypeToColumnFilter(StringType, 10)
    filter.update(InternalRow(UTF8String.fromString(value)), 0)
    filter
  }
}
