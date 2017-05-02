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
import com.github.sadikovi.riff.TypeDescription;
import com.github.sadikovi.riff.ntree.Rule;
import com.github.sadikovi.riff.ntree.Statistics;
import com.github.sadikovi.riff.ntree.Tree;

/**
 * [[Trivial]] node represents boolean flag `true` or `false`. Result for this node is already
 * known, and it is considered analyzed by default. All statistics and filter evaluation yields
 * the trivial value used to create node.
 */
public class Trivial implements Tree {
  private final boolean result;

  public Trivial(boolean result) {
    this.result = result;
  }

  /**
   * Get result for this node.
   * @return true or false
   */
  public boolean result() {
    return result;
  }

  @Override
  public boolean evaluateState(InternalRow row) {
    return result;
  }

  @Override
  public boolean evaluateState(Statistics[] stats) {
    return result;
  }

  @Override
  public boolean evaluateState(ColumnFilter[] filters) {
    return result;
  }

  @Override
  public Tree transform(Rule rule) {
    return rule.update(this);
  }

  @Override
  public Tree copy() {
    return new Trivial(result);
  }

  @Override
  public void analyze(TypeDescription td) {
    /* no-op */
  }

  @Override
  public boolean analyzed() {
    // trivial node is always analyzed
    return true;
  }

  @Override
  public String toString() {
    return "" + result;
  }
}
