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

package com.github.sadikovi.testutil

import scala.util.Try

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** General Spark base */
private[testutil] trait SparkBase {
  @transient private[testutil] var _spark: SparkSession = null

  def createSparkSession(): SparkSession

  /** Start (or create) Spark session */
  def startSparkSession(): Unit = {
    stopSparkSession()
    setLoggingLevel(Level.ERROR)
    _spark = createSparkSession()
  }

  /** Stop Spark session */
  def stopSparkSession(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
    _spark = null
  }

  /**
   * Set logging level globally for all.
   * Supported log levels:
   *      Level.OFF
   *      Level.ERROR
   *      Level.WARN
   *      Level.INFO
   * @param level logging level
   */
  def setLoggingLevel(level: Level) {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    Logger.getRootLogger().setLevel(level)
  }

  /** Returns Spark session */
  def spark: SparkSession = _spark

  /** Allow tests to set custom SQL configuration for duration of the closure */
  def withSQLConf(pairs: (String, String)*)(func: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try func finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /** Allow tests to set custom SQL configuration for duration of closure using map of options */
  def withSQLConf(options: Map[String, String])(func: => Unit): Unit = {
    withSQLConf(options.toSeq: _*)(func)
  }
}
