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

import org.apache.spark.sql.catalyst.InternalRow;

import com.github.sadikovi.riff.ColumnFilter;
import com.github.sadikovi.riff.TypeDescription;
import com.github.sadikovi.riff.TypeSpec;

/**
 * [[BoundReference]] represents leaf node with known name/ordinal. This is a leaf node for filters,
 * such as `IsNull` and other that do need value information for comparison. Subclasses might
 * overwrite default behaviours to bind name/ordinal to value expression.
 */
public abstract class BoundReference implements Tree {
  // alias for unresolved ordinal
  protected static final int UNRESOLVED_ORDINAL = -1;
  // internal ordinal, this should not be available for subclasses
  private int ordinal = UNRESOLVED_ORDINAL;

  /**
   * Column name for which this reference binds to.
   * @return column name
   */
  public abstract String name();

  /**
   * Evaluate state for internal row and ordinal.
   * Ordinal should be considered valid and should be used without modifications.
   * @param row row to evaluate
   * @param ordinal value ordinal
   * @return true if value at ordinal passes predicate, false otherwise
   */
  public abstract boolean evaluateState(InternalRow row, int ordinal);

  /**
   * Evaluate state for provided statistics instance.
   * This is optional optimization and we return true to always scan stripe.
   * @param stats statistics
   * @return true if predicate passes statistics, false otherwise
   */
  public abstract boolean evaluateState(Statistics stats);

  /**
   * Evaluate state for provided column filter. This is optional optimization, and by default
   * column filter is ignored and we return true to always scan stripe.
   * @param filter column filter
   * @return true if predicate passes column filter, false otherwise
   */
  public abstract boolean evaluateState(ColumnFilter filter);

  /**
   * Update current node using type spec information for the column node is associated with.
   * Default implementation does not update node.
   * @param spec type specification for the field
   */
  public void update(TypeSpec spec) {
    /* no-op */
    /* subclass overwrite */
  }

  /**
   * Ordinal for column in type description.
   * Only available if bound reference is analyzed.
   * @return ordinal value
   */
  public final int ordinal() {
    return ordinal;
  }

  /**
   * Return pretty name for column and ordinal that can be used by subclasses for `toString` method.
   * This reflects whether or not current bound reference is resolved.
   * @return pretty column name
   */
  protected String prettyName() {
    return analyzed() ? (name() + "[" + ordinal() + "]") : "*" + name();
  }

  @Override
  public final boolean evaluateState(InternalRow row) {
    return evaluateState(row, ordinal);
  }

  @Override
  public final boolean evaluateState(Statistics[] stats) {
    return evaluateState(stats[ordinal]);
  }

  @Override
  public final boolean evaluateState(ColumnFilter[] filters) {
    return evaluateState(filters[ordinal]);
  }

  @Override
  public final void analyze(TypeDescription td) {
    int pos = td.position(name());
    TypeSpec spec = td.atPosition(pos);
    ordinal = spec.position();
    update(spec);
  }

  @Override
  public final boolean analyzed() {
    return ordinal != UNRESOLVED_ORDINAL;
  }

  @Override
  public boolean equals(Object obj) {
    // equals method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    if (obj == null || obj.getClass() != this.getClass()) return false;
    BoundReference that = (BoundReference) obj;
    return name().equals(that.name()) && ordinal() == that.ordinal();
  }

  @Override
  public int hashCode() {
    // hashCode method is used only for testing to compare trees, it should never be used for
    // evaluating predicate
    int result = 31 * ordinal() + name().hashCode();
    return 31 * result + getClass().hashCode();
  }
}
