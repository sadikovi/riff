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

package com.github.sadikovi.riff.ntree.expression;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.ntree.BinaryLogical;
import com.github.sadikovi.riff.ntree.Rule;
import com.github.sadikovi.riff.ntree.Statistics;
import com.github.sadikovi.riff.ntree.Tree;

/**
 * [[And]] is binary logical predicate representing intersection of left and right subtrees -
 * only analyzed when both subtrees are analyzed. Evaluation for statistics and column filters
 * works the same way - if one of the children returns `false`, the result is `false`.
 */
public class And extends BinaryLogical {
  private final Tree left;
  private final Tree right;

  public And(Tree left, Tree right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public Tree left() {
    return left;
  }

  @Override
  public Tree right() {
    return right;
  }

  @Override
  public boolean evaluateState(InternalRow row) {
    return left.evaluateState(row) && right.evaluateState(row);
  }

  @Override
  public boolean evaluateState(Statistics[] stats) {
    return left.evaluateState(stats) && right.evaluateState(stats);
  }

  @Override
  public boolean evaluateState(ColumnFilter[] filters) {
    return left.evaluateState(filters) && right.evaluateState(filters);
  }

  @Override
  public Tree transform(Rule rule) {
    return rule.update(this);
  }

  @Override
  public Tree copy() {
    return new And(left.copy(), right.copy());
  }

  @Override
  public String toString() {
    return "(" + left + ") && (" + right + ")";
  }
}
