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

package com.github.sadikovi.riff.tree.expression

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.riff.column.ColumnFilter
import com.github.sadikovi.testutil.UnitTestSuite

class ExpressionSuite extends UnitTestSuite {
  test("IntegerExpression - misc methods") {
    val expr = new IntegerExpression(123)
    expr.dataType should be (IntegerType)
    expr.prettyString should be ("123")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new IntegerExpression(122)) should be (false)
    expr.equals(new IntegerExpression(123)) should be (true)
    expr.equals(new IntegerExpression(124)) should be (false)
    // hashCode
    expr.hashCode should be (123)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new IntegerExpression(122)) should be (1)
    expr.compareTo(new IntegerExpression(124)) should be (-1)
    // copy
    expr.copy() should be (expr)
  }

  test("IntegerExpression - check expression") {
    val expr = new IntegerExpression(123)
    // equality
    expr.eqExpr(InternalRow(123), 0) should be (true)
    expr.eqExpr(InternalRow(124), 0) should be (false)
    expr.eqExpr(InternalRow(122), 0) should be (false)
    expr.eqExpr(InternalRow(0), 0) should be (false)
    expr.eqExpr(InternalRow(Int.MaxValue), 0) should be (false)
    expr.eqExpr(InternalRow(Int.MinValue), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(123), 0) should be (false)
    expr.gtExpr(InternalRow(124), 0) should be (true)
    expr.gtExpr(InternalRow(122), 0) should be (false)
    expr.gtExpr(InternalRow(0), 0) should be (false)
    expr.gtExpr(InternalRow(Int.MaxValue), 0) should be (true)
    expr.gtExpr(InternalRow(Int.MinValue), 0) should be (false)
    // less than
    expr.ltExpr(InternalRow(123), 0) should be (false)
    expr.ltExpr(InternalRow(124), 0) should be (false)
    expr.ltExpr(InternalRow(122), 0) should be (true)
    expr.ltExpr(InternalRow(0), 0) should be (true)
    expr.ltExpr(InternalRow(Int.MaxValue), 0) should be (false)
    expr.ltExpr(InternalRow(Int.MinValue), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(123), 0) should be (true)
    expr.geExpr(InternalRow(124), 0) should be (true)
    expr.geExpr(InternalRow(122), 0) should be (false)
    expr.geExpr(InternalRow(0), 0) should be (false)
    expr.geExpr(InternalRow(Int.MaxValue), 0) should be (true)
    expr.geExpr(InternalRow(Int.MinValue), 0) should be (false)
    // less than or equal
    expr.leExpr(InternalRow(123), 0) should be (true)
    expr.leExpr(InternalRow(124), 0) should be (false)
    expr.leExpr(InternalRow(122), 0) should be (true)
    expr.leExpr(InternalRow(0), 0) should be (true)
    expr.leExpr(InternalRow(Int.MaxValue), 0) should be (false)
    expr.leExpr(InternalRow(Int.MinValue), 0) should be (true)
  }

  test("IntegerExpression - column filter check") {
    // column filter
    val filter = ColumnFilter.sqlTypeToColumnFilter(IntegerType, 16)
    filter.update(InternalRow(123), 0)
    new IntegerExpression(123).containsExpr(filter) should be (true)
    new IntegerExpression(999).containsExpr(filter) should be (false)
  }

  test("LongExpression - misc methods") {
    val expr = new LongExpression(512L)
    expr.dataType should be (LongType)
    expr.prettyString should be ("512L")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new LongExpression(457L)) should be (false)
    expr.equals(new LongExpression(512L)) should be (true)
    expr.equals(new LongExpression(-14L)) should be (false)
    // hashCode
    expr.hashCode should be (512)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new LongExpression(-14L)) should be (1)
    expr.compareTo(new LongExpression(617L)) should be (-1)
    // copy
    expr.copy() should be (expr)
  }

  test("LongExpression - check expression") {
    val expr = new LongExpression(512L)
    // equality
    expr.eqExpr(InternalRow(512L), 0) should be (true)
    expr.eqExpr(InternalRow(617L), 0) should be (false)
    expr.eqExpr(InternalRow(-14L), 0) should be (false)
    expr.eqExpr(InternalRow(0L), 0) should be (false)
    expr.eqExpr(InternalRow(Long.MaxValue), 0) should be (false)
    expr.eqExpr(InternalRow(Long.MinValue), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(512L), 0) should be (false)
    expr.gtExpr(InternalRow(617L), 0) should be (true)
    expr.gtExpr(InternalRow(-14L), 0) should be (false)
    expr.gtExpr(InternalRow(0L), 0) should be (false)
    expr.gtExpr(InternalRow(Long.MaxValue), 0) should be (true)
    expr.gtExpr(InternalRow(Long.MinValue), 0) should be (false)
    // less than
    expr.ltExpr(InternalRow(512L), 0) should be (false)
    expr.ltExpr(InternalRow(617L), 0) should be (false)
    expr.ltExpr(InternalRow(-14L), 0) should be (true)
    expr.ltExpr(InternalRow(0L), 0) should be (true)
    expr.ltExpr(InternalRow(Long.MaxValue), 0) should be (false)
    expr.ltExpr(InternalRow(Long.MinValue), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(512L), 0) should be (true)
    expr.geExpr(InternalRow(617L), 0) should be (true)
    expr.geExpr(InternalRow(-14L), 0) should be (false)
    expr.geExpr(InternalRow(0L), 0) should be (false)
    expr.geExpr(InternalRow(Long.MaxValue), 0) should be (true)
    expr.geExpr(InternalRow(Long.MinValue), 0) should be (false)
    // less than or equal
    expr.leExpr(InternalRow(512L), 0) should be (true)
    expr.leExpr(InternalRow(617L), 0) should be (false)
    expr.leExpr(InternalRow(-14L), 0) should be (true)
    expr.leExpr(InternalRow(0L), 0) should be (true)
    expr.leExpr(InternalRow(Long.MaxValue), 0) should be (false)
    expr.leExpr(InternalRow(Long.MinValue), 0) should be (true)
  }

  test("LongExpression - column filter check") {
    // column filter
    val filter = ColumnFilter.sqlTypeToColumnFilter(LongType, 16)
    filter.update(InternalRow(512L), 0)
    new LongExpression(512L).containsExpr(filter) should be (true)
    new LongExpression(999L).containsExpr(filter) should be (false)
  }

  test("UTF8StringExpression - misc methods") {
    val expr = new UTF8StringExpression(UTF8String.fromString("ttt"))
    expr.dataType should be (StringType)
    expr.prettyString should be ("'ttt'")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new UTF8StringExpression(UTF8String.fromString("ccc"))) should be (false)
    expr.equals(new UTF8StringExpression(UTF8String.fromString("ttt"))) should be (true)
    expr.equals(new UTF8StringExpression(UTF8String.fromString("zzz"))) should be (false)
    // hashCode
    expr.hashCode should be (337287601)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new UTF8StringExpression(UTF8String.fromString("ccc"))) should be (17)
    expr.compareTo(new UTF8StringExpression(UTF8String.fromString("zzz"))) should be (-6)
    // copy
    expr.copy() should be (expr)
  }

  test("UTF8StringExpression - check expression") {
    val expr = new UTF8StringExpression(UTF8String.fromString("ttt"))
    // equality
    expr.eqExpr(InternalRow(UTF8String.fromString("aaa")), 0) should be (false)
    expr.eqExpr(InternalRow(UTF8String.fromString("ttt")), 0) should be (true)
    expr.eqExpr(InternalRow(UTF8String.fromString("zzz")), 0) should be (false)
    expr.eqExpr(InternalRow(UTF8String.fromString("")), 0) should be (false)
    expr.eqExpr(InternalRow(UTF8String.fromString("ttt ")), 0) should be (false)
    expr.eqExpr(InternalRow(UTF8String.fromString(" ttt")), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(UTF8String.fromString("aaa")), 0) should be (false)
    expr.gtExpr(InternalRow(UTF8String.fromString("ttt")), 0) should be (false)
    expr.gtExpr(InternalRow(UTF8String.fromString("zzz")), 0) should be (true)
    expr.gtExpr(InternalRow(UTF8String.fromString("")), 0) should be (false)
    expr.gtExpr(InternalRow(UTF8String.fromString("ttt ")), 0) should be (true)
    expr.gtExpr(InternalRow(UTF8String.fromString(" ttt")), 0) should be (false)
    // less than
    expr.ltExpr(InternalRow(UTF8String.fromString("aaa")), 0) should be (true)
    expr.ltExpr(InternalRow(UTF8String.fromString("ttt")), 0) should be (false)
    expr.ltExpr(InternalRow(UTF8String.fromString("zzz")), 0) should be (false)
    expr.ltExpr(InternalRow(UTF8String.fromString("")), 0) should be (true)
    expr.ltExpr(InternalRow(UTF8String.fromString("ttt ")), 0) should be (false)
    expr.ltExpr(InternalRow(UTF8String.fromString(" ttt")), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(UTF8String.fromString("aaa")), 0) should be (false)
    expr.geExpr(InternalRow(UTF8String.fromString("ttt")), 0) should be (true)
    expr.geExpr(InternalRow(UTF8String.fromString("zzz")), 0) should be (true)
    expr.geExpr(InternalRow(UTF8String.fromString("")), 0) should be (false)
    expr.geExpr(InternalRow(UTF8String.fromString("ttt ")), 0) should be (true)
    expr.geExpr(InternalRow(UTF8String.fromString(" ttt")), 0) should be (false)
    // less than or equal
    expr.leExpr(InternalRow(UTF8String.fromString("aaa")), 0) should be (true)
    expr.leExpr(InternalRow(UTF8String.fromString("ttt")), 0) should be (true)
    expr.leExpr(InternalRow(UTF8String.fromString("zzz")), 0) should be (false)
    expr.leExpr(InternalRow(UTF8String.fromString("")), 0) should be (true)
    expr.leExpr(InternalRow(UTF8String.fromString("ttt ")), 0) should be (false)
    expr.leExpr(InternalRow(UTF8String.fromString(" ttt")), 0) should be (true)
  }

  test("UTF8StringExpression - column filter check") {
    // column filter
    val filter = ColumnFilter.sqlTypeToColumnFilter(StringType, 16)
    filter.update(InternalRow(UTF8String.fromString("ttt")), 0)
    new UTF8StringExpression(UTF8String.fromString("ttt")).containsExpr(filter) should be (true)
    new UTF8StringExpression(UTF8String.fromString("zzz")).containsExpr(filter) should be (false)
  }

  test("DateExpression - check expression") {
    // inherits comparison methods from IntegerExpression
    val time = 1234567890L
    val expr = new DateExpression(new Date(time))
    expr.dataType should be (DateType)
    expr.prettyString should be ("DATE(14)")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new DateExpression(new Date(time))) should be (true)
    expr.equals(new DateExpression(new Date(System.currentTimeMillis))) should be (false)
    // hashCode
    expr.hashCode should be (14)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new DateExpression(new Date(1L))) should be (1)
    expr.compareTo(new DateExpression(new Date(System.currentTimeMillis))) should be (-1)
    // copy
    assert(expr.copy().isInstanceOf[DateExpression])
    expr.copy() should be (expr)
  }

  test("TimestampExpression - check expression") {
    // inherits comparison methods from LongExpression
    val time = 1234567890L
    val expr = new TimestampExpression(new Timestamp(time))
    expr.dataType should be (TimestampType)
    expr.prettyString should be ("TIMESTAMP(1234567890000)")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new TimestampExpression(new Timestamp(time))) should be (true)
    expr.equals(new TimestampExpression(new Timestamp(System.currentTimeMillis))) should be (false)
    // hashCode
    expr.hashCode should be (1912276303)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new TimestampExpression(new Timestamp(1L))) should be (1)
    expr.compareTo(new TimestampExpression(new Timestamp(System.currentTimeMillis))) should be (-1)
    // copy
    assert(expr.copy().isInstanceOf[TimestampExpression])
    expr.copy() should be (expr)
  }

  test("BooleanExpression - misc methods") {
    val expr = new BooleanExpression(true)
    expr.dataType should be (BooleanType)
    expr.prettyString should be ("true")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new BooleanExpression(true)) should be (true)
    expr.equals(new BooleanExpression(false)) should be (false)
    // hashCode
    expr.hashCode should be (1)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new BooleanExpression(false)) should be (1)
    new BooleanExpression(false).compareTo(expr) should be (-1)
    // copy
    assert(expr.copy().isInstanceOf[BooleanExpression])
    expr.copy() should be (expr)
  }

  test("BooleanExpression - check expression") {
    val expr = new BooleanExpression(true)
    // equality
    expr.eqExpr(InternalRow(true), 0) should be (true)
    expr.eqExpr(InternalRow(false), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(true), 0) should be (false)
    expr.gtExpr(InternalRow(false), 0) should be (false)
    new BooleanExpression(false).gtExpr(InternalRow(true), 0) should be (true)
    // less than
    expr.ltExpr(InternalRow(true), 0) should be (false)
    expr.ltExpr(InternalRow(false), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(true), 0) should be (true)
    expr.geExpr(InternalRow(false), 0) should be (false)
    new BooleanExpression(false).geExpr(InternalRow(true), 0) should be (true)
    // less than or equal
    expr.leExpr(InternalRow(true), 0) should be (true)
    expr.leExpr(InternalRow(false), 0) should be (true)
    new BooleanExpression(false).leExpr(InternalRow(true), 0) should be (false)
  }

  test("ShortExpression - misc methods") {
    val expr = new ShortExpression(512.toShort)
    expr.dataType should be (ShortType)
    expr.prettyString should be ("512s")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new ShortExpression(457.toShort)) should be (false)
    expr.equals(new ShortExpression(512.toShort)) should be (true)
    expr.equals(new ShortExpression(-14.toShort)) should be (false)
    // hashCode
    expr.hashCode should be (512.toShort)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new ShortExpression(-14.toShort)) should be (1)
    expr.compareTo(new ShortExpression(617.toShort)) should be (-1)
    // copy
    expr.copy() should be (expr)
  }

  test("ShortExpression - check expression") {
    val expr = new ShortExpression(512.toShort)
    // equality
    expr.eqExpr(InternalRow(512.toShort), 0) should be (true)
    expr.eqExpr(InternalRow(617.toShort), 0) should be (false)
    expr.eqExpr(InternalRow(-14.toShort), 0) should be (false)
    expr.eqExpr(InternalRow(0.toShort), 0) should be (false)
    expr.eqExpr(InternalRow(Short.MaxValue), 0) should be (false)
    expr.eqExpr(InternalRow(Short.MinValue), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(512.toShort), 0) should be (false)
    expr.gtExpr(InternalRow(617.toShort), 0) should be (true)
    expr.gtExpr(InternalRow(-14.toShort), 0) should be (false)
    expr.gtExpr(InternalRow(0.toShort), 0) should be (false)
    expr.gtExpr(InternalRow(Short.MaxValue), 0) should be (true)
    expr.gtExpr(InternalRow(Short.MinValue), 0) should be (false)
    // less than
    expr.ltExpr(InternalRow(512.toShort), 0) should be (false)
    expr.ltExpr(InternalRow(617.toShort), 0) should be (false)
    expr.ltExpr(InternalRow(-14.toShort), 0) should be (true)
    expr.ltExpr(InternalRow(0.toShort), 0) should be (true)
    expr.ltExpr(InternalRow(Short.MaxValue), 0) should be (false)
    expr.ltExpr(InternalRow(Short.MinValue), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(512.toShort), 0) should be (true)
    expr.geExpr(InternalRow(617.toShort), 0) should be (true)
    expr.geExpr(InternalRow(-14.toShort), 0) should be (false)
    expr.geExpr(InternalRow(0.toShort), 0) should be (false)
    expr.geExpr(InternalRow(Short.MaxValue), 0) should be (true)
    expr.geExpr(InternalRow(Short.MinValue), 0) should be (false)
    // less than or equal
    expr.leExpr(InternalRow(512.toShort), 0) should be (true)
    expr.leExpr(InternalRow(617.toShort), 0) should be (false)
    expr.leExpr(InternalRow(-14.toShort), 0) should be (true)
    expr.leExpr(InternalRow(0.toShort), 0) should be (true)
    expr.leExpr(InternalRow(Short.MaxValue), 0) should be (false)
    expr.leExpr(InternalRow(Short.MinValue), 0) should be (true)
  }

  test("ShortExpression - column filter check") {
    val filter = ColumnFilter.sqlTypeToColumnFilter(ShortType, 16)
    new ShortExpression(512.toShort).containsExpr(filter) should be (false)
    new ShortExpression(999.toShort).containsExpr(filter) should be (false)
    filter.update(InternalRow(512.toShort), 0)
    filter.update(InternalRow(999.toShort), 0)
    new ShortExpression(512.toShort).containsExpr(filter) should be (true)
    new ShortExpression(999.toShort).containsExpr(filter) should be (true)
  }

  test("ByteExpression - misc methods") {
    val expr = new ByteExpression(51.toByte)
    expr.dataType should be (ByteType)
    expr.prettyString should be ("51b")
    // equals
    expr.equals(null) should be (false)
    expr.equals(expr) should be (true)
    expr.equals(new ByteExpression(45.toByte)) should be (false)
    expr.equals(new ByteExpression(51.toByte)) should be (true)
    expr.equals(new ByteExpression(-3.toByte)) should be (false)
    // hashCode
    expr.hashCode should be (51.toByte)
    // compareTo
    expr.compareTo(expr) should be (0)
    expr.compareTo(new ByteExpression(-3.toByte)) should be (1)
    expr.compareTo(new ByteExpression(61.toByte)) should be (-1)
    // copy
    expr.copy() should be (expr)
  }

  test("ByteExpression - check expression") {
    val expr = new ByteExpression(51.toByte)
    // equality
    expr.eqExpr(InternalRow(51.toByte), 0) should be (true)
    expr.eqExpr(InternalRow(61.toByte), 0) should be (false)
    expr.eqExpr(InternalRow(-3.toByte), 0) should be (false)
    expr.eqExpr(InternalRow(0.toByte), 0) should be (false)
    expr.eqExpr(InternalRow(Byte.MaxValue), 0) should be (false)
    expr.eqExpr(InternalRow(Byte.MinValue), 0) should be (false)
    // greater than
    expr.gtExpr(InternalRow(51.toByte), 0) should be (false)
    expr.gtExpr(InternalRow(61.toByte), 0) should be (true)
    expr.gtExpr(InternalRow(-3.toByte), 0) should be (false)
    expr.gtExpr(InternalRow(0.toByte), 0) should be (false)
    expr.gtExpr(InternalRow(Byte.MaxValue), 0) should be (true)
    expr.gtExpr(InternalRow(Byte.MinValue), 0) should be (false)
    // less than
    expr.ltExpr(InternalRow(51.toByte), 0) should be (false)
    expr.ltExpr(InternalRow(61.toByte), 0) should be (false)
    expr.ltExpr(InternalRow(-3.toByte), 0) should be (true)
    expr.ltExpr(InternalRow(0.toByte), 0) should be (true)
    expr.ltExpr(InternalRow(Byte.MaxValue), 0) should be (false)
    expr.ltExpr(InternalRow(Byte.MinValue), 0) should be (true)
    // greater than or equal
    expr.geExpr(InternalRow(51.toByte), 0) should be (true)
    expr.geExpr(InternalRow(61.toByte), 0) should be (true)
    expr.geExpr(InternalRow(-3.toByte), 0) should be (false)
    expr.geExpr(InternalRow(0.toByte), 0) should be (false)
    expr.geExpr(InternalRow(Byte.MaxValue), 0) should be (true)
    expr.geExpr(InternalRow(Byte.MinValue), 0) should be (false)
    // less than or equal
    expr.leExpr(InternalRow(51.toByte), 0) should be (true)
    expr.leExpr(InternalRow(61.toByte), 0) should be (false)
    expr.leExpr(InternalRow(-3.toByte), 0) should be (true)
    expr.leExpr(InternalRow(0.toByte), 0) should be (true)
    expr.leExpr(InternalRow(Byte.MaxValue), 0) should be (false)
    expr.leExpr(InternalRow(Byte.MinValue), 0) should be (true)
  }

  test("ByteExpression - column filter check") {
    val filter = ColumnFilter.sqlTypeToColumnFilter(ByteType, 16)
    new ByteExpression(43.toByte).containsExpr(filter) should be (false)
    new ByteExpression(-56.toByte).containsExpr(filter) should be (false)
    filter.update(InternalRow(43.toByte), 0)
    filter.update(InternalRow(-56.toByte), 0)
    new ByteExpression(43.toByte).containsExpr(filter) should be (true)
    new ByteExpression(-56.toByte).containsExpr(filter) should be (true)
  }
}
