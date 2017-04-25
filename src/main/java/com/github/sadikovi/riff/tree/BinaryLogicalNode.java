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
 * Binary logical node is a tree node that cannot be evaluated on its own, but using its children.
 * Mainly used as a container for concrete implementations by providing `equals` and `hashCode`
 * methods.
 */
public abstract class BinaryLogicalNode implements TreeNode {
  /**
   * Get left subtree.
   * @return left subtree
   */
  public abstract TreeNode left();

  /**
   * Get right subtree.
   * @return right subtree
   */
  public abstract TreeNode right();

  @Override
  public boolean resolved() {
    // both child nodes should be resolved
    return left().resolved() && right().resolved();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) return false;
    BinaryLogicalNode that = (BinaryLogicalNode) obj;
    return this.left().equals(that.left()) && this.right().equals(that.right());
  }

  @Override
  public int hashCode() {
    int result = left().hashCode();
    result = 31 * result + right().hashCode();
    result = 31 * result + getClass().hashCode();
    return result;
  }
}
