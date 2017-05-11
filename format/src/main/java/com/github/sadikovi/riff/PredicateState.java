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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.tree.BoundReference;
import com.github.sadikovi.riff.tree.Rule;
import com.github.sadikovi.riff.tree.State;
import com.github.sadikovi.riff.tree.Tree;

import com.github.sadikovi.riff.tree.expression.And;
import com.github.sadikovi.riff.tree.expression.GreaterThan;
import com.github.sadikovi.riff.tree.expression.GreaterThanOrEqual;
import com.github.sadikovi.riff.tree.expression.EqualTo;
import com.github.sadikovi.riff.tree.expression.In;
import com.github.sadikovi.riff.tree.expression.IsNull;
import com.github.sadikovi.riff.tree.expression.LessThan;
import com.github.sadikovi.riff.tree.expression.LessThanOrEqual;
import com.github.sadikovi.riff.tree.expression.Not;
import com.github.sadikovi.riff.tree.expression.Or;
import com.github.sadikovi.riff.tree.expression.Trivial;

/**
 * [[PredicateState]] keeps track of predicate tree and provides methods to resolve, update and
 * split tree on indexed and full.
 */
public class PredicateState {
  private static final Logger LOG = LoggerFactory.getLogger(PredicateState.class);

  // full resolved predicate tree
  private Tree tree;
  // resolved tree containing only index fields
  private final Tree indexTree;
  // whether or not state has only index tree
  private final boolean indexedOnly;
  // state for the tree
  private final State state;

  /**
   * Given resolved/unresolved tree and type description, perform resolution if possible and extract
   * index tree, if any available.
   */
  public PredicateState(Tree unresolvedTree, TypeDescription td) {
    if (unresolvedTree == null) throw new IllegalArgumentException("Tree is null");
    if (td == null) throw new IllegalArgumentException("Type description is null");
    // copy tree - analysis is done in place
    unresolvedTree = unresolvedTree.copy();
    // will be no-op if tree is already analyzed and resolved
    unresolvedTree.analyze(td);

    this.tree = unresolvedTree
      /* rule modifies existing tree to simplify */
      .transform(new IndexTreeBooleanSimplification());
    // this should never happen, tree should be resolved after applying rule, or exception will be
    // thrown during updates; but just in case changes are made to traversal or default behaviour.
    if (!this.tree.analyzed()) {
      throw new IllegalStateException("Tree " + this.tree + " is unresolved");
    }
    this.indexTree = this.tree
      /* rule returns copy of tree */
      .transform(new IndexFieldsExtract(td))
      /* rule copies and modifies tree by removing trivial branches */
      .transform(new IndexTreeBooleanSimplification());
    this.indexedOnly = this.indexTree.equals(this.tree);
    this.state = this.tree.state();
    if (this.indexedOnly) {
      // only maintain index tree at this point
      this.tree = null;
    }

    LOG.info("Index tree: {}, tree: {}, index_only: {}",
      this.indexTree, this.tree, this.indexedOnly);
  }

  /**
   * Return full resolved tree.
   * @return tree
   */
  public Tree tree() {
    return tree;
  }

  /**
   * Return resolved index tree.
   * @return index tree
   */
  public Tree indexTree() {
    return indexTree;
  }

  /**
   * Whether or not this state has only valid index tree. This allows us to skip second tree
   * evaluation when reading record with predicate.
   * @return true if state has only index tree (fields in that tree are indexed), false otherwise
   */
  public boolean hasIndexedTreeOnly() {
    return indexedOnly;
  }

  /**
   * Return state for the current active tree, either index tree or full tree. State determines
   * whether or not current tree can be evaluated in advance and trivial, or unknown and needs to
   * be evaluated for each row/statistics/filter.
   * @return state
   */
  public State result() {
    return state;
  }

  @Override
  public String toString() {
    return "State[" + state + ", index_tree=" + indexTree + ", tree=" + tree + ", index_only=" +
      indexedOnly + "]";
  }

  /**
   * Class is used to extract index fields and build tree, all data fields are replaced with
   * trivial nodes. Tree should be analyzed at this point.
   */
  static class IndexFieldsExtract implements Rule {
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
      Tree updated = node.child().transform(this);
      if (updated instanceof Trivial) return updated;
      return new Not(updated);
    }

    @Override
    public Tree update(Trivial node) {
      return node.copy();
    }
  }

  /**
   * Naive implementation of boolean simplification.
   * Right now this rule just finds logical nodes and simplifies them to bound references or
   * trivial nodes.
   * TODO: Introduce facility to add more such rules that can reduce predicate tree, e.g based on
   * statistics.
   */
  static class IndexTreeBooleanSimplification implements Rule {
    public IndexTreeBooleanSimplification() { }

    private Tree updateTree(Tree obj) {
      if (obj instanceof And) {
        And node = (And) obj;
        if (node.left() instanceof Trivial) {
          Trivial res = (Trivial) node.left();
          return res.result() ? node.right() : res;
        } else if (node.right() instanceof Trivial) {
          Trivial res = (Trivial) node.right();
          return res.result() ? node.left() : res;
        }
      } else if (obj instanceof Or) {
        Or node = (Or) obj;
        if (node.left() instanceof Trivial) {
          Trivial res = (Trivial) node.left();
          return res.result() ? res : node.right();
        } else if (node.right() instanceof Trivial) {
          Trivial res = (Trivial) node.right();
          return res.result() ? res : node.left();
        }
      } else if (obj instanceof Not) {
        Not node = (Not) obj;
        if (node.child() instanceof Trivial) {
          Trivial res = (Trivial) node.child();
          return new Trivial(!res.result());
        }
      }
      return obj;
    }

    @Override
    public Tree update(EqualTo node) {
      return node.copy();
    }

    @Override
    public Tree update(GreaterThan node) {
      return node.copy();
    }

    @Override
    public Tree update(LessThan node) {
      return node.copy();
    }

    @Override
    public Tree update(GreaterThanOrEqual node) {
      return node.copy();
    }

    @Override
    public Tree update(LessThanOrEqual node) {
      return node.copy();
    }

    @Override
    public Tree update(In node) {
      return node.copy();
    }

    @Override
    public Tree update(IsNull node) {
      return node.copy();
    }

    @Override
    public Tree update(And node) {
      return updateTree(new And(node.left().transform(this), node.right().transform(this)));
    }

    @Override
    public Tree update(Or node) {
      return updateTree(new Or(node.left().transform(this), node.right().transform(this)));
    }

    @Override
    public Tree update(Not node) {
      return updateTree(new Not(node.child().transform(this)));
    }

    @Override
    public Tree update(Trivial node) {
      return node.copy();
    }
  }
}
