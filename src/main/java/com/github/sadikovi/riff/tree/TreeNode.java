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

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.Statistics;

/**
 * Generic tree node.
 * Can represents entire predicate tree, and provides methods to evaluate tree for current row.
 * Note that tree node and children should be considered immutable and should not be modiified, but
 * new nodes should be returned instead.
 *
 * Each method returns boolean flag: `false` indicates that current row should be discarded or
 * current stripe should be discarded, because they do not pass predicate, `true` means that either
 * predicate passed or it is impossible to evaluate predicate (because there is not enough
 * information), in which case we just report it as correct. Note that if predicate or value is not
 * supported, or any other misuses or invalid data types, exception is raised.
 */
public interface TreeNode {
  /**
   * Evaluate current tree for row. Each leaf node should keep track of ordinal and type
   * information.
   * @param row row to evaluate
   * @return true if tree is successfully evaluated or unknown and false if row should be discarded
   */
  boolean evaluate(InternalRow row);

  /**
   * Evaluate tree for provided statistics. Statistics array comes from stripe information, each
   * ordinal of statistics instance corresponds to the ordinal of type spec - and matches ordinal
   * stored for each leaf node.
   * @param stats array of statistics
   * @return true if predicate is unknown or evaluated, false if it does not pass statistics
   */
  boolean evaluate(Statistics[] stats);

  /**
   * Transform current tree based on modifier. This should always return a copy of tree with
   * updates instead of updating current tree.
   * @param modifier modifier
   * @return a new tree
   */
  TreeNode transform(Modifier modifier);

  /**
   * Whether or not this predicate is ready for evaluation.
   * Right now it is used to indicate that all leaf node field names are resolved to their
   * corresponding ordinals using type description.
   * @return true if predicate fully resolved, false otherwise
   */
  boolean resolved();
}
