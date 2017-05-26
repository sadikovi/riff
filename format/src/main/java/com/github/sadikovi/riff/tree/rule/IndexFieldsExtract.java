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

package com.github.sadikovi.riff.tree.rule;

import com.github.sadikovi.riff.TypeDescription;
import com.github.sadikovi.riff.TypeSpec;
import com.github.sadikovi.riff.tree.BoundReference;
import com.github.sadikovi.riff.tree.Rule;
import com.github.sadikovi.riff.tree.Tree;

import com.github.sadikovi.riff.tree.node.EqualTo;
import com.github.sadikovi.riff.tree.node.GreaterThan;
import com.github.sadikovi.riff.tree.node.LessThan;
import com.github.sadikovi.riff.tree.node.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.node.LessThanOrEqual;
import com.github.sadikovi.riff.tree.node.In;
import com.github.sadikovi.riff.tree.node.IsNull;
import com.github.sadikovi.riff.tree.node.Not;
import com.github.sadikovi.riff.tree.node.And;
import com.github.sadikovi.riff.tree.node.Or;
import com.github.sadikovi.riff.tree.node.Trivial;

/**
 * Class is used to extract index fields and build tree, all data fields are replaced with
 * trivial nodes. Tree should be analyzed at this point.
 */
public class IndexFieldsExtract implements Rule {
  private final TypeDescription td;

  public IndexFieldsExtract(TypeDescription td) {
    this.td = td;
  }

  /** Return either trivial node or index field node for bound reference */
  private Tree updateRef(BoundReference ref) {
    TypeSpec spec = td.atPosition(ref.ordinal());
    // all non-indexed fields are replaced with `true`, this maintains correctness of the tree
    return spec.isIndexed() ? ref.copy() : new Trivial(true);
  }

  @Override
  public Tree update(EqualTo node) {
    return updateRef(node);
  }

  @Override
  public Tree update(GreaterThan node) {
    return updateRef(node);
  }

  @Override
  public Tree update(LessThan node) {
    return updateRef(node);
  }

  @Override
  public Tree update(GreaterThanOrEqual node) {
    return updateRef(node);
  }

  @Override
  public Tree update(LessThanOrEqual node) {
    return updateRef(node);
  }

  @Override
  public Tree update(In node) {
    return updateRef(node);
  }

  @Override
  public Tree update(IsNull node) {
    return updateRef(node);
  }

  @Override
  public Tree update(And node) {
    return new And(node.left().transform(this), node.right().transform(this));
  }

  @Override
  public Tree update(Or node) {
    return new Or(node.left().transform(this), node.right().transform(this));
  }

  @Override
  public Tree update(Not node) {
    // there is an issue when Not results in inversing correct trivial node, e.g.
    // Not(IsNull("col")), where "col" is a data field, meaning that IsNull("col") is replaced to
    // TRUE, and Not inverses result, meaning that 0 records will be returned.
    // In this case we should check on returned node
    // TODO: Check this update condition
    Tree updated = node.child().transform(this);
    if (updated instanceof Trivial) return updated;
    return new Not(updated);
  }

  @Override
  public Tree update(Trivial node) {
    return node.copy();
  }
}
