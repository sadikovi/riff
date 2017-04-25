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

package com.github.sadikovi.riff;

import com.github.sadikovi.riff.tree.BoundReference;
import com.github.sadikovi.riff.tree.Modifier;
import com.github.sadikovi.riff.tree.TreeNode;
import com.github.sadikovi.riff.tree.Tree.And;
import com.github.sadikovi.riff.tree.Tree.GreaterThan;
import com.github.sadikovi.riff.tree.Tree.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.Tree.EqualTo;
import com.github.sadikovi.riff.tree.Tree.In;
import com.github.sadikovi.riff.tree.Tree.IsNull;
import com.github.sadikovi.riff.tree.Tree.LessThan;
import com.github.sadikovi.riff.tree.Tree.LessThanOrEqual;
import com.github.sadikovi.riff.tree.Tree.Not;
import com.github.sadikovi.riff.tree.Tree.Or;
import com.github.sadikovi.riff.tree.Tree.Trivial;

/**
 * [[PredicateState]] keeps track of predicate tree and provides methods to resolve, update and
 * split tree on indexed and full.
 */
public class PredicateState {
  // full resolved predicate tree
  private TreeNode tree;
  // resolved tree containing only index fields
  private final TreeNode indexTree;
  // whether or not state has only index tree
  private final boolean indexedOnly;

  /**
   * Given raw unresolved tree and type description, perform resolution if possible and extract
   * index tree, if any available.
   */
  public PredicateState(TreeNode unresolvedTree, TypeDescription td) {
    if (unresolvedTree == null) throw new IllegalArgumentException("Tree is null");
    if (td == null) throw new IllegalArgumentException("Type description is null");
    if (unresolvedTree.resolved()) {
      throw new IllegalArgumentException("Expected unresolved tree, found " + unresolvedTree);
    }
    this.tree = unresolvedTree.transform(new TreeResolve(td));
    this.indexTree = this.tree.transform(new IndexFieldsExtract(td));
    this.indexedOnly = this.indexTree.equals(this.tree);
    if (this.indexedOnly) {
      // only maintain index tree at this point
      this.tree = null;
    }
  }

  /**
   * Return full resolved tree.
   * @return tree
   */
  public TreeNode tree() {
    return tree;
  }

  /**
   * Return resolved index tree.
   * @return index tree
   */
  public TreeNode indexTree() {
    return indexTree;
  }

  /**
   * Whether or not this state has only valid index tree. This allows us to skip second tree
   * evaluation when reading record with predicate.
   * @return true if state has only index tree (all fields are index fields), false otherwise
   */
  public boolean hasIndexedTreeOnly() {
    return indexedOnly;
  }

  /**
   * Resolve tree using type description.
   */
  static class TreeResolve implements Modifier {
    private final TypeDescription td;

    TreeResolve(TypeDescription td) {
      this.td = td;
    }

    /** Update ordinal for bound reference */
    private BoundReference updateRef(BoundReference ref) {
      TypeSpec spec = td.atPosition(td.position(ref.name()));
      if (!spec.dataType().equals(ref.dataType())) {
        throw new IllegalStateException("Type mismatch: ref=" + ref + ", spec=" + spec);
      }
      return ref.withOrdinal(spec.position());
    }

    @Override
    public TreeNode update(EqualTo node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(GreaterThan node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(LessThan node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(GreaterThanOrEqual node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(LessThanOrEqual node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(In node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(IsNull node) {
      // IsNull does not have value and has NullType, which is why it is handled differently
      // without type checking
      TypeSpec spec = td.atPosition(td.position(node.name()));
      return node.withOrdinal(spec.position());
    }

    @Override
    public TreeNode update(And node) {
      return new And(node.left().transform(this), node.right().transform(this));
    }

    @Override
    public TreeNode update(Or node) {
      return new Or(node.left().transform(this), node.right().transform(this));
    }

    @Override
    public TreeNode update(Not node) {
      return new Not(node.child().transform(this));
    }

    @Override
    public TreeNode update(Trivial node) {
      return new Trivial(node.result());
    }
  }

  /**
   * Class is used to extract index fields and build tree, all data fields are replaced with
   * trivial nodes. Tree should be resolved at this point.
   */
  static class IndexFieldsExtract implements Modifier {
    private final TypeDescription td;

    public IndexFieldsExtract(TypeDescription td) {
      this.td = td;
    }

    /** Return either trivial node or index field node for bound reference */
    private TreeNode updateRef(BoundReference ref) {
      TypeSpec spec = td.atPosition(ref.ordinal());
      return spec.isIndexed() ? ref : new Trivial(true);
    }

    @Override
    public TreeNode update(EqualTo node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(GreaterThan node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(LessThan node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(GreaterThanOrEqual node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(LessThanOrEqual node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(In node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(IsNull node) {
      return updateRef(node);
    }

    @Override
    public TreeNode update(And node) {
      return new And(node.left().transform(this), node.right().transform(this));
    }

    @Override
    public TreeNode update(Or node) {
      return new Or(node.left().transform(this), node.right().transform(this));
    }

    @Override
    public TreeNode update(Not node) {
      return new Not(node.child().transform(this));
    }

    @Override
    public TreeNode update(Trivial node) {
      return new Trivial(node.result());
    }
  }
}
