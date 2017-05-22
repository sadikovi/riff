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
 * Naive implementation of boolean simplification.
 * Right now this rule just finds logical nodes and simplifies them to bound references or
 * trivial nodes.
 * TODO: Introduce facility to add more such rules that can reduce predicate tree, e.g based on
 * statistics.
 */
public class BooleanSimplification implements Rule {
  public BooleanSimplification() { }

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
