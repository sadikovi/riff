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

package com.github.sadikovi.riff

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.github.sadikovi.testutil.UnitTestSuite

class TypeSpecSuite extends UnitTestSuite {
  test("initialize type spec 1") {
    val field = StructField("col", IntegerType)
    val (indexed, pos, origPos) = (false, 0, 1)
    val spec = new TypeSpec(field, indexed, pos, origPos)

    spec.field() should be (field)
    spec.dataType() should be (field.dataType)
    spec.isIndexed should be (indexed)
    spec.position() should be (pos)
    spec.origSQLPos() should be (origPos)
  }

  test("initialize type spec 2") {
    val field = StructField("col", StringType)
    val (indexed, pos, origPos) = (true, 10, 4)
    val spec = new TypeSpec(field, indexed, pos, origPos)

    spec.field() should be (field)
    spec.dataType() should be (field.dataType)
    spec.isIndexed should be (indexed)
    spec.position() should be (pos)
    spec.origSQLPos() should be (origPos)
  }

  test("check toString") {
    val field = StructField("col", StringType)
    val (indexed, pos, origPos) = (true, 10, 4)
    val spec = new TypeSpec(field, indexed, pos, origPos)
    val expected = s"TypeSpec(${field.name}: ${field.dataType.simpleString}, indexed=$indexed, " +
      s"position=$pos, origPos=$origPos)"
    spec.toString should be (expected)
  }

  test("equals") {
    val field = StructField("col", StringType)
    val (indexed, pos, origPos) = (true, 1, 3)
    val spec = new TypeSpec(field, indexed, pos, origPos)
    spec.equals(spec) should be (true)

    val spec1 = new TypeSpec(field, indexed, pos, origPos);
    spec.equals(spec1) should be (true)
    val spec2 = new TypeSpec(field, false, pos, origPos);
    spec.equals(spec2) should be (false)
    val spec3 = new TypeSpec(field, true, pos + 1, origPos);
    spec.equals(spec3) should be (false)
    val spec4 = new TypeSpec(field, true, pos, origPos + 1);
    spec.equals(spec4) should be (false)
  }

  test("hashCode") {
    val field = StructField("col", StringType)
    val (indexed, pos, origPos) = (true, 1, 3)
    val spec = new TypeSpec(field, indexed, pos, origPos)
    spec.hashCode() should be (818348067)

    val spec1 = new TypeSpec(field, false, pos, origPos);
    assert(spec1.hashCode != spec.hashCode)
  }

  test("write/read spec into external stream 1") {
    val field = StructField("col", StringType)
    val (indexed, pos, origPos) = (true, 1, 3)
    val spec1 = new TypeSpec(field, indexed, pos, origPos)

    val out = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(out)
    oos.writeObject(spec1)

    val in = new ByteArrayInputStream(out.toByteArray)
    val ois = new ObjectInputStream(in)
    val spec2 = ois.readObject().asInstanceOf[TypeSpec]

    spec2 should be (spec1)
    spec2.isIndexed() should be (true)
    spec2.position() should be (1)
    spec2.origSQLPos() should be (3)
  }

  test("write/read spec into external stream 2") {
    val field = StructField("field",
      StructType(StructField("col", IntegerType, true) :: Nil))
    val (indexed, pos, origPos) = (false, 5, 2)
    val spec1 = new TypeSpec(field, indexed, pos, origPos)

    val out = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(out)
    oos.writeObject(spec1)

    val in = new ByteArrayInputStream(out.toByteArray)
    val ois = new ObjectInputStream(in)
    val spec2 = ois.readObject().asInstanceOf[TypeSpec]

    spec2 should be (spec1)
    spec2.isIndexed() should be (false)
    spec2.position() should be (5)
    spec2.origSQLPos() should be (2)
  }
}
