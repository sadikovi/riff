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

/**
 * Predicate state.
 * Right now is only reported to capture initial state of the tree.
 * True state - tree is trivial and results in `true` (filter passes)
 * False state - tree is trivial and results in `false` (filter does not pass)
 * Unknown state - tree is not trivial and evaluation is required
 */
public enum State {
  True, False, Unknown;

  /**
   * Construct trivial state from boolean flag.
   * Used for Trivial node.
   * @param result result
   * @return True or False
   */
  public static State trivial(boolean result) {
    return result ? True : False;
  }

  /**
   * Intersection on states.
   * @param that other state
   * @return merged state
   */
  public State and(State that) {
    if (this == Unknown || that == Unknown) return Unknown;
    if (this == False || that == False) return False;
    return True;
  }

  /**
   * Union on states.
   * @param that other state
   * @return merged state
   */
  public State or(State that) {
    if (this == True || that == True) return True;
    if (this == Unknown || that == Unknown) return Unknown;
    return False;
  }
}
