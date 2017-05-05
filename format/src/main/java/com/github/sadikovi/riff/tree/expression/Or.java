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

package com.github.sadikovi.riff.tree.expression;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.Statistics;
import com.github.sadikovi.riff.tree.BinaryLogical;
import com.github.sadikovi.riff.tree.Rule;
import com.github.sadikovi.riff.tree.Tree;

/**
 * [[Or]] is binary logical node representing union of child subtrees. Node is analyzed when both
 * children are analyzed. Evaluated when either or both of the children yield `true` as a result.
 */
public class Or extends BinaryLogical {
  private final Tree left;
  private final Tree right;

  public Or(Tree left, Tree right) {
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
    return left.evaluateState(row) || right.evaluateState(row);
  }

  @Override
  public boolean evaluateState(Statistics[] stats) {
    return left.evaluateState(stats) || right.evaluateState(stats);
  }

  @Override
  public boolean evaluateState(ColumnFilter[] filters) {
    return left.evaluateState(filters) || right.evaluateState(filters);
  }

  @Override
  public Tree transform(Rule rule) {
    return rule.update(this);
  }

  @Override
  public Tree copy() {
    return new Or(left.copy(), right.copy());
  }

  @Override
  public String toString() {
    return "(" + left + ") || (" + right + ")";
  }
}
