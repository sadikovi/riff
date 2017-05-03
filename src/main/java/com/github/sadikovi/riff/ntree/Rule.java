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

package com.github.sadikovi.riff.ntree;

import com.github.sadikovi.riff.ntree.expression.EqualTo;
import com.github.sadikovi.riff.ntree.expression.GreaterThan;
import com.github.sadikovi.riff.ntree.expression.LessThan;
import com.github.sadikovi.riff.ntree.expression.GreaterThanOrEqual;
import com.github.sadikovi.riff.ntree.expression.LessThanOrEqual;
import com.github.sadikovi.riff.ntree.expression.In;
import com.github.sadikovi.riff.ntree.expression.IsNull;
import com.github.sadikovi.riff.ntree.expression.Not;
import com.github.sadikovi.riff.ntree.expression.And;
import com.github.sadikovi.riff.ntree.expression.Or;
import com.github.sadikovi.riff.ntree.expression.Trivial;

/**
 * Rule allows to traverse tree and return updated/modified tree which contains either copies of
 * nodes or completely different subtrees. It is mainly used to replace types of the subtrees,
 * rather than node values.
 */
public interface Rule {
  Tree update(EqualTo node);

  Tree update(GreaterThan node);

  Tree update(LessThan node);

  Tree update(GreaterThanOrEqual node);

  Tree update(LessThanOrEqual node);

  Tree update(In node);

  Tree update(IsNull node);

  Tree update(Not node);

  Tree update(And node);

  Tree update(Or node);

  Tree update(Trivial node);
}
