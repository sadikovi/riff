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

public class Riff {
  public static final String MAGIC = "RIFF";

  /**
   * Internal riff options that can be set in hadoop configuration.
   */
  public static class Options {
    // Number of rows in single stripe, this is used for writing only
    public static final String STRIPE_ROWS = "riff.stripe.rows";
    public static final int STRIPE_ROWS_DEFAULT = 10000;

    // buffer size in bytes
    public static final String BUFFER_SIZE = "riff.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 256 * 1024;
    public static final int BUFFER_SIZE_MIN = 4 * 1024;
    public static final int BUFFER_SIZE_MAX = 512 * 1024;
  }

  private Riff() { }
}
