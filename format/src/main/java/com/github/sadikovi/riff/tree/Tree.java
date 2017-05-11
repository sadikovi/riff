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

package com.github.sadikovi.riff.tree;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.Statistics;
import com.github.sadikovi.riff.TypeDescription;

/**
 * [[Tree]] is a generic predicate tree node.
 * Provides methods to evaluate this tree and all its children. Every tree node should be
 * considered immutable when transforming. State is evaluated for internal row, considered always
 * resolved and returns `true` when internal row passes predicate or `false` otherwise.
 */
public interface Tree extends Serializable {
  /**
   * Evaluate state for this tree.
   * @param row row to evaluate
   * @return true if row passes predicate, false otherwise
   */
  boolean evaluateState(InternalRow row);

  /**
   * Evaluate tree for provided statistics. Statistics array comes from stripe information, each
   * ordinal of statistics instance corresponds to the ordinal of type spec - and matches ordinal
   * stored for each leaf node.
   * @param stats array of statistics
   * @return true if statistics pass predicate, false otherwise
   */
  boolean evaluateState(Statistics[] stats);

  /**
   * Evaluate tree for provided non-null array of column filters. They are loaded as part of stripe
   * information, and each ordinal of filter instance corresponds to the ordinal of type spec - and
   * matches ordinal stored for each leaf node. Each filter is guaranteed to be non-null.
   * @param filters array of column filters
   * @return true if filters pass predicate, false otherwise
   */
  boolean evaluateState(ColumnFilter[] filters);

  /**
   * Transform current tree using rule.
   * This should always return full copy of the tree.
   * @param rule rule to use for transform
   * @return copy of this tree
   */
  Tree transform(Rule rule);

  /**
   * Return a full copy of this node and all its children.
   * @return copy of the tree
   */
  Tree copy();

  /**
   * Analyze this tree for current type description. Type description is evaluated for bound
   * reference only; throws exception if tree cannot be analyzed. This method should be run before
   * evaluating this tree.
   * @param td type description
   */
  void analyze(TypeDescription td);

  /**
   * Whether or not this tree and all its subsequent children is analyzed.
   * @return true if tree is analyzed, false otherwise
   */
  boolean analyzed();

  /**
   * Return state for the tree.
   * Each node can return either True, False, or Unknown. True means that tree can be evaluated
   * without check and has `true` value, similar for `false`. Unknown state means that tree needs
   * to be evaluated for each row/statistics/filter to determine result.
   */
  State state();
}
