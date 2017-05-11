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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import com.github.sadikovi.spark.riff._

object ScanBenchmark {
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

  private def scanBenchmark(spark: SparkSession): Unit = {
    val valuesPerIteration = 1000000

    val fs = new Path("./temp").getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path("./temp/parquet-table"), true)
    fs.delete(new Path("./temp/orc-table"), true)
    fs.delete(new Path("./temp/riff-table"), true)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(0 until valuesPerIteration, 40).map(row), schema)
    df.write.parquet("./temp/parquet-table")
    df.write.orc("./temp/orc-table")
    df.write.option("index", "col1,col3,col5").riff("./temp/riff-table")

    val scanBenchmark1 = new Benchmark("SQL scan (int, all fields)", valuesPerIteration)
    // read is for default settings in Spark
    scanBenchmark1.addCase("Parquet") { iter =>
      spark.read.parquet("./temp/parquet-table")
        .filter("col1 >= 10 and col1 <= 100000").foreach(_ => Unit)
    }

    scanBenchmark1.addCase("ORC") { iter =>
      spark.read.orc("./temp/orc-table")
        .filter("col1 >= 10 and col1 <= 100000").foreach(_ => Unit)
    }

    scanBenchmark1.addCase("Riff") { iter =>
      spark.read.riff("./temp/riff-table")
        .filter("col1 >= 10 and col1 <= 100000").foreach(_ => Unit)
    }

    val scanBenchmark2 = new Benchmark("SQL scan (string, all fields)", valuesPerIteration)
    scanBenchmark2.addCase("Parquet") { iter =>
      spark.read.parquet("./temp/parquet-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .foreach(_ => Unit)
    }

    scanBenchmark2.addCase("ORC") { iter =>
      spark.read.orc("./temp/orc-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .foreach(_ => Unit)
    }

    scanBenchmark2.addCase("Riff") { iter =>
      spark.read.riff("./temp/riff-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .foreach(_ => Unit)
    }

    val scanBenchmark3 = new Benchmark("SQL scan (string, projection)", valuesPerIteration)
    // read is for default settings in Spark
    scanBenchmark3.addCase("Parquet") { iter =>
      spark.read.parquet("./temp/parquet-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .select("col2", "col4")
        .foreach(_ => Unit)
    }

    scanBenchmark3.addCase("ORC") { iter =>
      spark.read.orc("./temp/orc-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .select("col2", "col4")
        .foreach(_ => Unit)
    }

    scanBenchmark3.addCase("Riff") { iter =>
      spark.read.riff("./temp/riff-table")
        .filter("col5 >= 'abc9 abc9 abc9' and col1 <= 'abc10000 abc10000 abc10000'")
        .select("col2", "col4")
        .foreach(_ => Unit)
    }

    scanBenchmark1.run
    scanBenchmark2.run
    scanBenchmark3.run
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[4]").
      setAppName("spark-scan-benchmark")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    scanBenchmark(spark)
    spark.stop()
  }
}
