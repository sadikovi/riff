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

import java.io.{InputStream, OutputStream}
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.testutil.implicits._

trait TestBase {
  val RESOLVER = "path-resolver"

  var path: String = ""

  // local file system for tests
  val fs = FileSystem.get(new HadoopConf(false))

  /** returns raw path of the folder where it finds resolver */
  private def getRawPath(): String = {
    if (path.isEmpty) {
      path = getClass.getResource("/" + RESOLVER).getPath()
    }
    path
  }

  /** base directory of the project */
  final protected def baseDirectory(): String = {
    val original = getRawPath().split("/")
    require(original.length > 4, s"Path length is too short (<= 4): ${original.length}")
    val base = original.dropRight(4)
    var dir = ""
    for (suffix <- base) {
      if (suffix.nonEmpty) {
        dir = dir / suffix
      }
    }
    dir
  }

  /** main directory of the project (./src/main) */
  final protected def mainDirectory(): String = {
    baseDirectory() / "src" / "main"
  }

  /** test directory of the project (./src/test) */
  final protected def testDirectory(): String = {
    baseDirectory() / "src" / "test"
  }

  /** target directory of the project (./target) */
  final protected def targetDirectory(): String = {
    baseDirectory() / "target"
  }

  /** Create directories for path recursively */
  final protected def mkdirs(path: String): Boolean = {
    mkdirs(new HadoopPath(path))
  }

  final protected def mkdirs(path: HadoopPath): Boolean = {
    fs.mkdirs(path)
  }

  /** Create empty file, similar to "touch" shell command, but creates intermediate directories */
  final protected def touch(path: String): Boolean = {
    touch(new HadoopPath(path))
  }

  final protected def touch(path: HadoopPath): Boolean = {
    fs.mkdirs(path.getParent)
    fs.createNewFile(path)
  }

  /** Delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: String, recursive: Boolean): Boolean = {
    rm(new HadoopPath(path), recursive)
  }

  /** Delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: HadoopPath, recursive: Boolean): Boolean = {
    fs.delete(path, recursive)
  }

  /** Open file for a path */
  final protected def open(path: String): InputStream = {
    open(new HadoopPath(path))
  }

  final protected def open(path: HadoopPath): InputStream = {
    fs.open(path)
  }

  /** Create file with a path and return output stream */
  final protected def create(path: String): OutputStream = {
    create(new HadoopPath(path))
  }

  final protected def create(path: HadoopPath): OutputStream = {
    fs.create(path)
  }

  /** Compare two DataFrame objects */
  final protected def checkAnswer(df: DataFrame, expected: DataFrame): Unit = {
    // show nicely formatted schema
    def schema2str(schema: StructType): String = schema.map { field =>
      s"${field.name}: ${field.dataType.simpleString}" }.mkString("[", ", ", "]")
    // we do not preserve nullability
    if (df.dtypes.toList != expected.dtypes.toList) {
      throw new AssertionError(
        s"Schema mismatch, expected ${schema2str(df.schema)}, found ${schema2str(expected.schema)}")
    }
    val seq1 = df.collect.map { row => (row, row.toString) }.sortBy(_._2)
    val seq2 = expected.collect.map { row => (row, row.toString) }.sortBy(_._2)

    if (seq1.length != seq2.length) {
      throw new AssertionError(s"""
        | ${seq1.length} rows != ${seq2.length} rows
        | == DataFrame ==
        | ${seq1.map(_._2).mkString("[", ", ", "]")}
        | == Expected ==
        | ${seq2.map(_._2).mkString("[", ", ", "]")}
        """.stripMargin)
    }

    // check row values
    def checkRow(row1: Row, row2: Row): Unit = {
      for (i <- 0 until row1.length) {
        if (row1.get(i) != row2.get(i)) {
          throw new AssertionError(s"$row1 != $row2, reason: ${row1.get(i)} != ${row2.get(i)}")
        }
      }
    }

    for (i <- 0 until seq1.length) {
      checkRow(seq1(i)._1, seq2(i)._1)
    }
  }

  final protected def checkAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    val sc = df.sqlContext.sparkContext
    checkAnswer(df, df.sqlContext.createDataFrame(sc.parallelize(expected), df.schema))
  }

  /** Create temporary directory on local file system */
  private def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "riff"): HadoopPath = {
    val dir = new HadoopPath(root / namePrefix / UUID.randomUUID().toString)
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path and path permission */
  private def withTempHadoopPath(path: HadoopPath, permission: Option[FsPermission])
      (func: HadoopPath => Unit): Unit = {
    try {
      if (permission.isDefined) {
        fs.setPermission(path, permission.get)
      }
      func(path)
    } finally {
      fs.delete(path, true)
    }
  }

  /** Execute code block with created temporary directory with provided permission */
  def withTempDir(permission: FsPermission)(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), Some(permission))(func)
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), None)(func)
  }
}
