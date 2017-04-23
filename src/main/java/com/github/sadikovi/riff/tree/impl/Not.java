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
import com.github.sadikovi.riff.tree.UnaryLogicalNode;

public class Not extends UnaryLogicalNode {
  private final TreeNode child;

  public Not(TreeNode child) {
    this.child = child;
  }

  @Override
  public TreeNode child() {
    return child;
  }

  @Override
  public boolean evaluate(InternalRow row) {
    return !child.evaluate(row);
  }

  @Override
  public boolean evaluate(Statistics[] stats) {
    return !child.evaluate(stats);
  }

  @Override
  public TreeNode transform(Modifier modifier) {
    return modifier.update(this);
  }

  @Override
  public String toString() {
    return "!(" + child + ")";
  }
}