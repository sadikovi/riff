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

package com.github.sadikovi.riff.tree.node;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.stats.Statistics;
import com.github.sadikovi.riff.tree.Rule;
import com.github.sadikovi.riff.tree.State;
import com.github.sadikovi.riff.tree.Tree;
import com.github.sadikovi.riff.tree.UnaryLogical;

/**
 * [[Not]] is unary logical node that inverses value of the child tree. Note that in some cases
 * we keep `true` instead of inversing child's value - this is done to avoid false reporting of
 * filters.
 */
public class Not extends UnaryLogical {
  private final Tree child;

  public Not(Tree child) {
    this.child = child;
  }

  @Override
  public Tree child() {
    return child;
  }

  @Override
  public boolean evaluateState(InternalRow row) {
    return !child.evaluateState(row);
  }

  @Override
  public boolean evaluateState(Statistics[] stats) {
    // `not` does not evaluate statistics, because child expression evaluation does not guarantee
    // that records for inverse result will not exist in dataset
    return true;
  }

  @Override
  public boolean evaluateState(ColumnFilter[] filters) {
    // `not` does not evaluate column filters, because child expression evaluation does not
    // guarantee that records for inverse result will not exist in dataset
    return true;
  }

  @Override
  public State state() {
    // state of not predicate is unknown similar to evaluation
    return State.Unknown;
  }

  @Override
  public Tree transform(Rule rule) {
    return rule.update(this);
  }

  @Override
  public Tree copy() {
    return new Not(child.copy());
  }

  @Override
  public String toString() {
    return "!(" + child + ")";
  }
}
