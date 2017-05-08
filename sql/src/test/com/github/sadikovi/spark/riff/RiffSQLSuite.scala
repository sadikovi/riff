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

import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.{SparkLocal, UnitTestSuite}

class RiffSQLSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll() {
    startSparkSession
  }

  override def afterAll() {
    stopSparkSession
  }

  private val format = "com.github.sadikovi.spark.riff"

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

      df.write.format(format).option("index", "col2").save(dir.toString / "table")
      val res = spark.read.format(format).load(dir.toString / "table")
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

      df.write.format(format).option("index", "col2")
        .partitionBy("col1", "col3")
        .save(dir.toString / "table")
      val res = spark.read.format(format).load(dir.toString / "table")
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
        df.write.format(format).option("index", "col3")
          .partitionBy("col1", "col3")
          .save(dir.toString / "table")
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

      df.write.format(format).option("index", "col2").save(dir.toString / "table")
      var left = spark.read.format(format).load(dir.toString / "table").filter("col1 = 1")
      var right = df.filter("col1 = 1").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.format(format).load(dir.toString / "table").filter("col2 = 'abc4'")
      right = df.filter("col2 = 'abc4'").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.format(format).load(dir.toString / "table").filter("col3 = 3L")
      right = df.filter("col3 = 3L").select("col2", "col1", "col3")
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

      df.write.format(format).option("index", "col2").save(dir.toString / "table")
      var left = spark.read.format(format).load(dir.toString / "table")
        .filter("col1 between 1 and 4")
      var right = df.filter("col1 between 1 and 4").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.format(format).load(dir.toString / "table")
        .filter("col2 between 'abc1' and 'abc3'")
      right = df.filter("col2 between 'abc1' and 'abc3'").select("col2", "col1", "col3")
      checkAnswer(left, right)

      left = spark.read.format(format).load(dir.toString / "table")
        .filter("col3 between 2L and 4L")
      right = df.filter("col3 between 2L and 4L").select("col2", "col1", "col3")
      checkAnswer(left, right)

      // filter using In statement
      left = spark.read.format(format).load(dir.toString / "table")
        .filter("col2 in ('abc1', 'abc3')")
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

      df.write.format(format).option("index", "col2").save(dir.toString / "table")
      val left = spark.read.format(format).load(dir.toString / "table").filter("col1 is null")
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

      df.write.format(format).option("index", "col2,col4").save(dir.toString / "table")
      val left = spark.read.format(format).load(dir.toString / "table")
        .select("col4", "col2", "col1", "col5")
      val right = df.select("col4", "col2", "col1", "col5")
      checkAnswer(left, right)
    }
  }
}
