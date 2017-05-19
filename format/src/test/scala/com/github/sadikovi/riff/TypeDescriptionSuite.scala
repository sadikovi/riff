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

import java.io.ByteArrayInputStream
import java.util.NoSuchElementException

import org.apache.spark.sql.types._

import com.github.sadikovi.riff.io.OutputBuffer
import com.github.sadikovi.testutil.UnitTestSuite

class TypeDescriptionSuite extends UnitTestSuite {
  test("assert schema - schema is null") {
    val schema: StructType = null
    val err = intercept[UnsupportedOperationException] {
      new TypeDescription(schema, Array("col1", "col2"))
    }
    err.getMessage should be (
      s"Schema has insufficient number of columns, $schema, expected at least one column")
  }

  test("assert schema - schema is empty") {
    val schema = StructType(Nil)
    val err = intercept[UnsupportedOperationException] {
      new TypeDescription(schema, Array("col1", "col2"))
    }
    err.getMessage should be (
      s"Schema has insufficient number of columns, $schema, expected at least one column")
  }

  test("supported types") {
    TypeDescription.isSupportedDataType(IntegerType) should be (true)
    TypeDescription.isSupportedDataType(LongType) should be (true)
    TypeDescription.isSupportedDataType(StringType) should be (true)
    TypeDescription.isSupportedDataType(DateType) should be (true)
    TypeDescription.isSupportedDataType(TimestampType) should be (true)
    TypeDescription.isSupportedDataType(BooleanType) should be (true)
  }

  test("unsupported types") {
    TypeDescription.isSupportedDataType(ShortType) should be (false)
    TypeDescription.isSupportedDataType(ByteType) should be (false)
    TypeDescription.isSupportedDataType(DoubleType) should be (false)
    TypeDescription.isSupportedDataType(NullType) should be (false)
  }

  test("assert schema - unsupported type") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) ::
      StructField("col4", NullType) :: Nil)
    val err = intercept[UnsupportedOperationException] {
      new TypeDescription(schema, Array("col1", "col2"))
    }
    err.getMessage should be (
      "Field StructField(col4,NullType,true) with type NullType is not supported")
  }

  test("init type description - duplicate column names in indexed fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) :: Nil)

    val err = intercept[IllegalArgumentException] {
      new TypeDescription(schema, Array("col1", "col2", "col1"))
    }
    err.getMessage should be ("Found duplicate index column 'col1' in list [col1, col2, col1]")
  }

  test("init type description - duplicate indexed fields in schema") {
    // col2 is duplicated that is part of the indexed fields [col1, col2]
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col2", IntegerType) ::
      StructField("col3", StringType) :: Nil)
    val err = intercept[RuntimeException] {
      new TypeDescription(schema, Array("col1", "col2"))
    }
    assert(err.getMessage.contains("Inconsistency of schema with type description"))
  }

  test("init type description - duplicate data fields in schema") {
    // col3 is duplicated that is not part of the indexed fields [col1, col2]
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col3", StringType) :: Nil)
    val err = intercept[RuntimeException] {
      new TypeDescription(schema, Array("col1", "col2"))
    }
    assert(err.getMessage.contains("Inconsistency of schema with type description"))
  }

  test("init type description - indexed fields as null") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) :: Nil)
    val td = new TypeDescription(schema, null)
    td.size() should be (3)
    td.indexFields().isEmpty should be (true)
  }

  test("init type description - indexed fields as empty array") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) :: Nil)
    val td = new TypeDescription(schema, Array.empty)
    td.size() should be (3)
    td.indexFields().isEmpty should be (true)
  }

  test("init type description with indexed fields - check fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col4", "col1"))

    td.size() should be (5)

    td.indexFields() should be (Array(
      new TypeSpec(StructField("col4", StringType), true, 0, 3),
      new TypeSpec(StructField("col1", IntegerType), true, 1, 0)
    ))

    td.dataFields() should be (Array(
      new TypeSpec(StructField("col2", LongType), false, 2, 1),
      new TypeSpec(StructField("col3", IntegerType), false, 3, 2),
      new TypeSpec(StructField("col5", StringType), false, 4, 4)
    ))

    td.fields() should be (Array(
      new TypeSpec(StructField("col4", StringType), true, 0, 3),
      new TypeSpec(StructField("col1", IntegerType), true, 1, 0),
      new TypeSpec(StructField("col2", LongType), false, 2, 1),
      new TypeSpec(StructField("col3", IntegerType), false, 3, 2),
      new TypeSpec(StructField("col5", StringType), false, 4, 4)
    ))
  }

  test("init type description with indexed fields only - check fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col3", "col1", "col2"))

    td.size() should be (3)

    td.indexFields() should be (Array(
      new TypeSpec(StructField("col3", StringType), true, 0, 2),
      new TypeSpec(StructField("col1", IntegerType), true, 1, 0),
      new TypeSpec(StructField("col2", LongType), true, 2, 1)
    ))

    td.dataFields() should be (Array.empty)

    td.fields() should be (Array(
      new TypeSpec(StructField("col3", StringType), true, 0, 2),
      new TypeSpec(StructField("col1", IntegerType), true, 1, 0),
      new TypeSpec(StructField("col2", LongType), true, 2, 1)
    ))
  }

  test("init type description with data fields only - check fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", StringType) :: Nil)
    val td = new TypeDescription(schema, null)

    td.size() should be (3)

    td.indexFields() should be (Array.empty)

    td.dataFields() should be (Array(
      new TypeSpec(StructField("col1", IntegerType), false, 0, 0),
      new TypeSpec(StructField("col2", LongType), false, 1, 1),
      new TypeSpec(StructField("col3", StringType), false, 2, 2)
    ))

    td.fields() should be (Array(
      new TypeSpec(StructField("col1", IntegerType), false, 0, 0),
      new TypeSpec(StructField("col2", LongType), false, 1, 1),
      new TypeSpec(StructField("col3", StringType), false, 2, 2)
    ))
  }

  test("type description - position by name") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col4", "col1"))

    td.position("col4") should be (0)
    td.position("col1") should be (1)
    td.position("col2") should be (2)
    td.position("col3") should be (3)
    td.position("col5") should be (4)

    val err = intercept[NoSuchElementException] {
      td.position("abc")
    }
    err.getMessage should be ("No such field abc")
  }

  test("type description - atPosition by ordinal") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col4", "col1"))

    td.atPosition(0) should be (new TypeSpec(StructField("col4", StringType), true, 0, 3))
    td.atPosition(1) should be (new TypeSpec(StructField("col1", IntegerType), true, 1, 0))
    td.atPosition(2) should be (new TypeSpec(StructField("col2", LongType), false, 2, 1))
    td.atPosition(3) should be (new TypeSpec(StructField("col3", IntegerType), false, 3, 2))
    td.atPosition(4) should be (new TypeSpec(StructField("col5", StringType), false, 4, 4))
  }

  test("type description - equals 1") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) :: Nil)
    val td1 = new TypeDescription(schema, Array("col1"))
    val td2 = new TypeDescription(schema, Array("col1"))
    val td3 = new TypeDescription(schema, Array("col1", "col2"))
    val td4 = new TypeDescription(schema)

    td1.equals(td1) should be (true)
    td2.equals(td1) should be (true)
    td3.equals(td1) should be (false)
    td4.equals(td1) should be (false)
  }

  test("type description - equals 2") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) :: Nil)
    val td1 = new TypeDescription(schema, Array("col1"))
    val td2 = new TypeDescription(schema, Array("col1"))
    td2 should be (td1)
  }

  test("type description - toString") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) :: Nil)
    val td = new TypeDescription(schema, Array("col1"))
    td.toString should be ("TypeDescription[" +
      "TypeSpec(col1: int, indexed=true, position=0, origPos=0), " +
      "TypeSpec(col2: bigint, indexed=false, position=1, origPos=1), " +
      "TypeSpec(col3: int, indexed=false, position=2, origPos=2)]")
  }

  test("write/read type description") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td1 = new TypeDescription(schema, Array("col4", "col1"))
    val out = new OutputBuffer()
    td1.writeTo(out)

    val in = new ByteArrayInputStream(out.array())
    val td2 = TypeDescription.readFrom(in)
    td2.equals(td1) should be (true)
    td2.toString should be (td1.toString)
  }

  test("write/read type description, data fields only") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td1 = new TypeDescription(schema)
    val out = new OutputBuffer()
    td1.writeTo(out)

    val in = new ByteArrayInputStream(out.array())
    val td2 = TypeDescription.readFrom(in)
    td2.equals(td1) should be (true)
    td2.toString should be (td1.toString)
  }

  test("write/read type description, index fields only") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", StringType) ::
      StructField("col3", StringType) :: Nil)
    val td1 = new TypeDescription(schema, Array("col1", "col2", "col3"))
    val out = new OutputBuffer()
    td1.writeTo(out)

    val in = new ByteArrayInputStream(out.array())
    val td2 = TypeDescription.readFrom(in)
    td2.equals(td1) should be (true)
    td2.toString should be (td1.toString)
  }

  test("convert to struct type") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) :: Nil)
    val td = new TypeDescription(schema, Array("col4", "col1", "col5"))
    td.toStructType() should be (StructType(
      StructField("col4", StringType) ::
      StructField("col1", IntegerType) ::
      StructField("col5", StringType) ::
      StructField("col2", LongType) ::
      StructField("col3", IntegerType) :: Nil))
  }

  test("convert to struct type without index fields") {
    val schema = StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) :: Nil)
    val td = new TypeDescription(schema)
    td.toStructType() should be (schema)
  }
}
