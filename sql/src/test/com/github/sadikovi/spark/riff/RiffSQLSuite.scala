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

package com.github.sadikovi.spark.riff

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.{SparkLocal, UnitTestSuite}

class RiffSQLSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll() {
    startSparkSession
  }

  override def afterAll() {
    stopSparkSession
  }

  test("parse index fields for option") {
    RiffFileFormat.parseIndexFields(null) should be (Array.empty)
    RiffFileFormat.parseIndexFields("") should be (Array.empty)
    RiffFileFormat.parseIndexFields(",") should be (Array.empty)
    RiffFileFormat.parseIndexFields(",,,") should be (Array.empty)
    RiffFileFormat.parseIndexFields("col1,col2,col3") should be (Array("col1", "col2", "col3"))
    RiffFileFormat.parseIndexFields("col1, col2, col3") should be (Array("col1", "col2", "col3"))
    RiffFileFormat.parseIndexFields("   col1   ") should be (Array("col1"))
    RiffFileFormat.parseIndexFields("col1, , col3") should be (Array("col1", "col3"))
  }

  test("write/read riff non-partitioned table") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2"),
        (3, 30L, "abc3"),
        (4, 40L, "abc4"),
        (5, 50L, "abc5")
      ).toDF("col1", "col2", "col3")

      df.write.option("index", "col2").riff(dir.toString / "table")
      val res = spark.read.riff(dir.toString / "table")
      // column order changes because of the indexing
      checkAnswer(res, df.select("col2", "col1", "col3"))
      // check that metadata exists
      assert(fs.exists(dir / "table" / "_riff_metadata"))
    }
  }

  test("write/read riff partitioned table") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2"),
        (3, 30L, "abc3"),
        (4, 40L, "abc4"),
        (5, 50L, "abc5")
      ).toDF("col1", "col2", "col3")

      df.write.option("index", "col2").partitionBy("col1", "col3").riff(dir.toString / "table")
      val res = spark.read.riff(dir.toString / "table")
      // column order changes because of the indexing
      checkAnswer(res, df.select("col2", "col1", "col3"))
      assert(fs.exists(dir / "table" / "_riff_metadata"))
    }
  }

  test("fail to index partition column") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2")
      ).toDF("col1", "col2", "col3")

      val err = intercept[IllegalArgumentException] {
        df.write.option("index", "col3").partitionBy("col1", "col3").riff(dir.toString / "table")
      }
      assert(err.getMessage.contains("Field \"col3\" does not exist"))
    }
  }

  test("filter single row") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2"),
        (3, 30L, "abc3"),
        (4, 40L, "abc4"),
        (5, 50L, "abc5")
      ).toDF("col1", "col2", "col3")

      df.write.option("index", "col2").riff(dir.toString / "table")
      var left = spark.read.riff(dir.toString / "table").filter("col1 = 1")
      var right = df.filter("col1 = 1").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.riff(dir.toString / "table").filter("col3 = 'abc4'")
      right = df.filter("col3 = 'abc4'").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.riff(dir.toString / "table").filter("col2 = 3L")
      right = df.filter("col2 = 3L").select("col2", "col1", "col3")
      checkAnswer(left, right)
    }
  }

  test("filter range") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2"),
        (3, 30L, "abc3"),
        (4, 40L, "abc4"),
        (5, 50L, "abc5")
      ).toDF("col1", "col2", "col3")

      df.write.option("index", "col2").riff(dir.toString / "table")
      var left = spark.read.riff(dir.toString / "table").filter("col1 between 1 and 4")
      var right = df.filter("col1 between 1 and 4").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.riff(dir.toString / "table").filter("col3 between 'abc1' and 'abc3'")
      right = df.filter("col3 between 'abc1' and 'abc3'").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.riff(dir.toString / "table").filter("col2 between 2L and 4L")
      right = df.filter("col2 between 2L and 4L").select("col2", "col1", "col3")
      checkAnswer(left, right)

      // filter using In statement
      left = spark.read.riff(dir.toString / "table").filter("col2 in ('abc1', 'abc3')")
      right = df.filter("col2 in ('abc1', 'abc3')").select("col2", "col1", "col3")
      checkAnswer(left, right)
    }
  }

  test("filter with result of empty rows") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1"),
        (2, 20L, "abc2"),
        (3, 30L, "abc3"),
        (4, 40L, "abc4"),
        (5, 50L, "abc5")
      ).toDF("col1", "col2", "col3")

      df.write.option("index", "col2").riff(dir.toString / "table")
      val left = spark.read.riff(dir.toString / "table").filter("col1 is null")
      val right = df.filter("col1 is null").select("col2", "col1", "col3")
      checkAnswer(left, right)
    }
  }

  test("project correct column order") {
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq(
        (1, 10L, "abc1", "def1", 100),
        (2, 20L, "abc2", "def2", 200),
        (3, 30L, "abc3", "def3", 300),
        (4, 40L, "abc4", "def4", 400),
        (5, 50L, "abc5", "def5", 500)
      ).toDF("col1", "col2", "col3", "col4", "col5")

      df.write.option("index", "col2,col4").riff(dir.toString / "table")
      val left = spark.read.riff(dir.toString / "table").select("col4", "col2", "col1", "col5")
      val right = df.select("col4", "col2", "col1", "col5")
      checkAnswer(left, right)
    }
  }

  test("write/read dataframe with null values + column filter") {
    // this test checks that column filters are updated without errors when null values are
    // present for indexed columns
    val implicits = spark.implicits
    withTempDir { dir =>
      import implicits._
      val df = Seq[(String, java.lang.Integer, java.lang.Long)](
        ("a", 1, 2L),
        (null, null, null),
        ("c", -1, 4L)
      ).toDF("col1", "col2", "col3").coalesce(1)

      df.write.option("index", "col1,col2,col3").riff(dir.toString / "table")
      val res = spark.read.riff(dir.toString / "table")

      checkAnswer(res.filter("col2 = 1"), df.filter("col2 = 1"))
      checkAnswer(res.filter("col2 = -1"), df.filter("col2 = -1"))
      checkAnswer(res.filter("col2 is null"), df.filter("col2 is null"))
      checkAnswer(res.filter("col3 = 2"), df.filter("col3 = 2"))
      checkAnswer(res.filter("col3 = 4"), df.filter("col3 = 4"))
      checkAnswer(res.filter("col3 is null"), df.filter("col3 is null"))
    }
  }

  test("check metadata count") {
    // default number of records in stripe
    withSQLConf(RiffFileFormat.SQL_RIFF_METADATA_COUNT -> "true") {
      withTempDir { dir =>
        spark.range(1237).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (1237)
      }

      withTempDir { dir =>
        spark.range(0).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (0)
      }

      withTempDir { dir =>
        spark.range(99999).write.riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (99999)
      }
    }
    // small number of records in stripe
    withSQLConf(
        RiffFileFormat.SQL_RIFF_METADATA_COUNT -> "true",
        RiffFileFormat.SQL_RIFF_STRIPE_ROWS -> "12") {
      withTempDir { dir =>
        spark.range(1200).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (1200)
      }

      withTempDir { dir =>
        spark.range(11).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (11)
      }

      withTempDir { dir =>
        spark.range(99999).write.riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").count should be (99999)
      }
    }
  }

  test("check metadata count + filter") {
    withSQLConf(RiffFileFormat.SQL_RIFF_METADATA_COUNT -> "true") {
      withTempDir { dir =>
        spark.range(1237).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").filter("id < 100").count should be (100)
      }

      withTempDir { dir =>
        spark.range(0).write.option("index", "id").riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").filter("id = -1").count should be (0)
      }

      withTempDir { dir =>
        spark.range(99999).write.riff(dir.toString / "table")
        spark.read.riff(dir.toString / "table").filter("id != 0").count should be (99998)
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // == Write/read tests for different datatypes
  //////////////////////////////////////////////////////////////

  /**
   * Method to check write/read for a provided data type and set of values.
   * Also allows to provide generic transformation that is applied on actual and expected results.
   */
  def checkDataType(
      dataType: DataType,
      rows: Seq[Row],
      func: DataFrame => DataFrame = null): Unit = {
    withTempDir { dir =>
      val schema = StructType(StructField("col", dataType) :: Nil)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      df.write.riff(dir.toString / "table")
      val res = spark.read.riff(dir.toString / "table")
      if (func == null) {
        checkAnswer(res, df)
      } else {
        checkAnswer(func(res), func(df))
      }
    }
  }

  test("write/read dataframe, IntegerType") {
    val seq = Row(0) :: Row(2) :: Row(3) :: Row(4) :: Row(null) :: Nil
    checkDataType(IntegerType, seq)
    checkDataType(IntegerType, seq, _.filter("col = 4"))
  }

  test("write/read dataframe, LongType") {
    val seq = Row(0L) :: Row(2L) :: Row(3L) :: Row(4L) :: Row(null) :: Nil
    checkDataType(LongType, seq)
    checkDataType(LongType, seq, _.filter("col = 4"))
  }

  test("write/read dataframe, StringType") {
    val seq = Row("a") :: Row("b") :: Row("c") :: Row("d") :: Row(null) :: Nil
    checkDataType(StringType, seq)
    checkDataType(StringType, seq, _.filter("col = 'c'"))
  }

  test("write/read dataframe, DateType") {
    val seq = Row(new Date(1000000L)) :: Row(new Date(2000000L)) :: Row(new Date(3000000L)) ::
      Row(null) :: Nil
    checkDataType(DateType, seq)
    checkDataType(DateType, seq, _.filter("col is null"))
    checkDataType(DateType, seq, _.filter(col("col") === new Date(1000000L)))
  }

  test("write/read dataframe, TimestampType") {
    val seq = Row(new Timestamp(10000L)) :: Row(new Timestamp(20000L)) ::
      Row(new Timestamp(30000L)) :: Row(null) :: Nil
    checkDataType(TimestampType, seq)
    checkDataType(TimestampType, seq, _.filter("col is null"))
    checkDataType(TimestampType, seq, _.filter(col("col") === new Timestamp(20000L)))
  }

  test("write/read dataframe, BooleanType") {
    val seq = Row(true) :: Row(false) :: Row(null) :: Nil
    checkDataType(BooleanType, seq)
    checkDataType(BooleanType, seq, _.filter("col is null"))
    checkDataType(BooleanType, seq, _.filter(col("col") === true))
  }

  test("write/read dataframe, ShortType") {
    val seq = Row(12345.toShort) :: Row(-671.toShort) :: Row(null) :: Nil
    checkDataType(ShortType, seq)
    checkDataType(ShortType, seq, _.filter("col is null"))
    checkDataType(ShortType, seq, _.filter(col("col") === -671.toShort))
  }

  test("write/read dataframe, ByteType") {
    val seq = Row(51.toByte) :: Row(-67.toByte) :: Row(null) :: Nil
    checkDataType(ByteType, seq)
    checkDataType(ByteType, seq, _.filter("col is null"))
    checkDataType(ByteType, seq, _.filter(col("col") === -67.toByte))
  }

  //////////////////////////////////////////////////////////////
  // == Write/read checks for compression codecs
  //////////////////////////////////////////////////////////////

  test("write/read dataframe, snappy") {
    withSQLConf(RiffFileFormat.SQL_RIFF_COMPRESSION_CODEC -> "snappy") {
      withTempDir { dir =>
        val df = spark.range(1237).select(
          col("id").as("col1"),
          col("id").cast("string").as("col2"),
          col("id").cast("bigint").as("col3"))
        df.write.option("index", "col1").riff(dir.toString / "table")
        // check that folder contains files with snappy extension
        val ls = fs.listStatus(dir / "table").map(_.getPath.getName).filter { path =>
          path.startsWith("part-") && path.contains(".snappy") }
        ls.isEmpty should be (false)
        // check result with input DataFrame
        val res = spark.read.riff(dir.toString / "table")
        checkAnswer(res, df)
      }
    }
  }

  test("write/read dataframe, gzip") {
    withSQLConf(RiffFileFormat.SQL_RIFF_COMPRESSION_CODEC -> "gzip") {
      withTempDir { dir =>
        val df = spark.range(1237).select(
          col("id").as("col1"),
          col("id").cast("string").as("col2"),
          col("id").cast("bigint").as("col3"))
        df.write.option("index", "col1").riff(dir.toString / "table")
        // check that folder contains files with gzip extension
        val ls = fs.listStatus(dir / "table").map(_.getPath.getName).filter { path =>
          path.startsWith("part-") && path.contains(".gz") }
        ls.isEmpty should be (false)
        val res = spark.read.riff(dir.toString / "table")
        checkAnswer(res, df)
      }
    }
  }

  test("write/read dataframe, deflate") {
    withSQLConf(RiffFileFormat.SQL_RIFF_COMPRESSION_CODEC -> "deflate") {
      withTempDir { dir =>
        val df = spark.range(1237).select(
          col("id").as("col1"),
          col("id").cast("string").as("col2"),
          col("id").cast("bigint").as("col3"))
        df.write.option("index", "col1").riff(dir.toString / "table")
        // check that folder contains files with deflate extension
        val ls = fs.listStatus(dir / "table").map(_.getPath.getName).filter { path =>
          path.startsWith("part-") && path.contains(".deflate") }
        ls.isEmpty should be (false)
        val res = spark.read.riff(dir.toString / "table")
        checkAnswer(res, df)
      }
    }
  }
}
