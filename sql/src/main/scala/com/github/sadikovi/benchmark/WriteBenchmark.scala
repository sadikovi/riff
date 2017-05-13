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

package com.github.sadikovi.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import com.github.sadikovi.spark.riff._

object WriteBenchmark {
  val schema = StructType(
    StructField("col1", IntegerType) ::
    StructField("col2", IntegerType) ::
    StructField("col3", LongType) ::
    StructField("col4", LongType) ::
    StructField("col5", StringType) ::
    StructField("col6", StringType) ::
    StructField("col7", StringType) ::
    StructField("col8", StringType) :: Nil)

  // method to generate dummy row
  def row(i: Int): Row = {
    Row(i, i, i.toLong, i.toLong, s"abc$i abc$i abc$i", s"abc$i abc$i abc$i",
      s"abc$i abc$i abc$i", s"abc$i abc$i abc$i")
  }

  private def writeBenchmark(spark: SparkSession): Unit = {
    val valuesPerIteration = 1000000
    val numPartitions = 50

    val writeBenchmark = new Benchmark("SQL Write", valuesPerIteration)
    writeBenchmark.addCase("Parquet write, gzip") { iter =>
      spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(0 until valuesPerIteration, numPartitions).map(row), schema)
      df.write.mode("overwrite").parquet("./temp/parquet-table")
    }

    writeBenchmark.addCase("ORC write, zlib/deflate") { iter =>
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(0 until valuesPerIteration, numPartitions).map(row), schema)
      df.write.mode("overwrite").option("compression", "ZLIB").orc("./temp/orc-table")
    }

    writeBenchmark.addCase("Riff write (+column filters), gzip") { iter =>
      spark.conf.set("spark.sql.riff.compression.codec", "gzip")
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(0 until valuesPerIteration, numPartitions).map(row), schema)
      df.write.mode("overwrite").option("index", "col1,col3,col5").riff("./temp/riff-table")
    }

    writeBenchmark.addCase("Riff write (+column filters), deflate") { iter =>
      spark.conf.set("spark.sql.riff.compression.codec", "deflate")
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(0 until valuesPerIteration, numPartitions).map(row), schema)
      df.write.mode("overwrite").option("index", "col1,col3,col5").riff("./temp/riff-table")
    }

    writeBenchmark.addCase("Riff write (+column filters), snappy") { iter =>
      spark.conf.set("spark.sql.riff.compression.codec", "snappy")
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(0 until valuesPerIteration, numPartitions).map(row), schema)
      df.write.mode("overwrite").option("index", "col1,col3,col5").riff("./temp/riff-table")
    }

    writeBenchmark.run
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[4]").
      setAppName("spark-write-benchmark")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    writeBenchmark(spark)
    spark.stop()
  }
}
