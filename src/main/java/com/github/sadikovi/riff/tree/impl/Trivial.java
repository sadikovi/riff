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

package com.github.sadikovi.riff.tree.impl;

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.Statistics;
import com.github.sadikovi.riff.tree.Modifier;
import com.github.sadikovi.riff.tree.TreeNode;

public class Trivial implements TreeNode {
  private final boolean result;

  public Trivial(boolean result) {
    this.result = result;
  }

  /**
   * Return result for this trivial filter.
   * @return true or false
   */
  public boolean result() {
    return result;
  }

  @Override
  public boolean evaluate(InternalRow row) {
    return result;
  }

  @Override
  public boolean evaluate(Statistics[] stats) {
    return result;
  }

  @Override
  public TreeNode transform(Modifier modifier) {
    return modifier.update(this);
  }

  @Override
  public boolean resolved() {
    // trivial predicate is always resolved, since result is known
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) return false;
    Trivial that = (Trivial) obj;
    return that.result() == this.result();
  }

  @Override
  public int hashCode() {
    return 31 + (result ? 1 : 0);
  }

  @Override
  public String toString() {
    return "" + result;
  }
}
