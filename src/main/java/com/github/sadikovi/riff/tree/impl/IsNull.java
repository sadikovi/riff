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
import com.github.sadikovi.riff.tree.LeafNode;
import com.github.sadikovi.riff.tree.Modifier;
import com.github.sadikovi.riff.tree.TreeNode;

public class IsNull extends LeafNode {
  public IsNull(String name, int ordinal) {
    super(name, ordinal);
  }

  @Override
  protected boolean evaluate(InternalRow row, int ordinal) {
    return row.isNullAt(ordinal);
  }

  @Override
  public TreeNode transform(Modifier modifier) {
    return modifier.update(this);
  }

  @Override
  public boolean statUpdate(boolean hasNulls) {
    // this filter represents search for a value for this field which is null,
    // if statistics does not have nulls, we should return false
    return hasNulls;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) return false;
    IsNull that = (IsNull) obj;
    return that.ordinal() == ordinal() && name().equals(that.name());
  }

  @Override
  public int hashCode() {
    return 31 * ordinal + name.hashCode();
  }

  @Override
  protected String toString(String tag) {
    return tag + " is null";
  }
}
