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
import com.github.sadikovi.riff.tree.BinaryLogicalNode;
import com.github.sadikovi.riff.tree.Modifier;
import com.github.sadikovi.riff.tree.TreeNode;

public class And extends BinaryLogicalNode {
  private final TreeNode left;
  private final TreeNode right;

  public And(TreeNode left, TreeNode right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public TreeNode left() {
    return left;
  }

  @Override
  public TreeNode right() {
    return right;
  }

  @Override
  public boolean evaluate(InternalRow row) {
    return left.evaluate(row) && right.evaluate(row);
  }

  @Override
  public boolean evaluate(Statistics[] stats) {
    return left.evaluate(stats) && right.evaluate(stats);
  }

  @Override
  public TreeNode transform(Modifier modifier) {
    return modifier.update(this);
  }

  @Override
  public String toString() {
    return "(" + left + " && " + right + ")";
  }
}
