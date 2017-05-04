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

import com.github.sadikovi.riff.TypeDescription;

/**
 * Unary logical node that has single subtree.
 * This class serves as a container by providing generic `equals` and `hashCode` methods.
 */
public abstract class UnaryLogical implements Tree {
  /**
   * Get child tree for this unary node.
   * @return subtree
   */
  public abstract Tree child();

  @Override
  public final void analyze(TypeDescription td) {
    child().analyze(td);
  }

  @Override
  public final boolean analyzed() {
    return child().analyzed();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) return false;
    UnaryLogical that = (UnaryLogical) obj;
    return this.child().equals(that.child());
  }

  @Override
  public int hashCode() {
    int result = child().hashCode();
    result = 31 * result + getClass().hashCode();
    return result;
  }
}
