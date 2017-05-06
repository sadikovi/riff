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

package com.github.sadikovi.spark.riff

import org.apache.spark.sql.sources._

import com.github.sadikovi.riff.tree.Tree
import com.github.sadikovi.riff.tree.FilterApi._

/**
 * Utility functions to convert Spark SQL filters into Riff filters.
 */
private[riff] object Filters {
  /**
   * Create new Riff filter for SQL filter.
   * @param filter optional filter
   * @return predicate tree or null if filter is None
   */
  def createRiffFilter(filter: Option[Filter]): Tree = {
    if (filter.isDefined) {
      recurBuild(filter.get)
    } else {
      null
    }
  }

  /**
   * Recursively build tree.
   * Tree is not analyzed at this point.
   */
  private def recurBuild(filter: Filter): Tree = {
    filter match {
      case EqualTo(attribute: String, value: Any) =>
        eqt(attribute, value)
      case EqualNullSafe(attribute: String, value: Any) =>
        // for us we treat `EqualNullSafe` as  `EqualTo` or `IsNull` depending on value
        if (value == null) nvl(attribute) else eqt(attribute, value)
      case GreaterThan(attribute: String, value: Any) =>
        gt(attribute, value)
      case GreaterThanOrEqual(attribute: String, value: Any) =>
        ge(attribute, value)
      case LessThan(attribute: String, value: Any) =>
        lt(attribute, value)
      case LessThanOrEqual(attribute: String, value: Any) =>
        le(attribute, value)
      case In(attribute: String, values: Array[Any]) =>
        // scala does not like passing Any into vargs
        in(attribute, values.map(_.asInstanceOf[AnyRef]))
      case IsNull(attribute: String) =>
        nvl(attribute)
      case IsNotNull(attribute: String) =>
        not(nvl(attribute))
      case And(left: Filter, right: Filter) =>
        and(recurBuild(left), recurBuild(right))
      case Or(left: Filter, right: Filter) =>
        or(recurBuild(left), recurBuild(right))
      case Not(child: Filter) =>
        not(recurBuild(child))
      case other =>
        // for unsupported predicates return `true`,
        // relying on Spark to do additional filtering
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        TRUE
    }
  }
}
