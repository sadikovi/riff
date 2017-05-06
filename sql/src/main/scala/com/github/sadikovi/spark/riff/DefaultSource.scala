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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.riff.{RiffOutputWriter, RiffOutputWriterFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

import com.github.sadikovi.riff.Riff.Options
import com.github.sadikovi.riff.TypeDescription
import com.github.sadikovi.riff.io.CompressionCodecFactory
import com.github.sadikovi.riff.hadoop.RiffOutputCommitter

/**
 * Spark SQL datasource for Riff file format.
 */
class DefaultSource
  extends FileFormat
  with DataSourceRegister
  with Serializable {

  import RiffFileFormat._

  // logger for file format, since we cannot use internal Spark logging
  @transient private val log = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "riff"

  override def toString(): String = "Riff"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(obj: Any): Boolean = obj.isInstanceOf[DefaultSource]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // create type description and try extracting index fields if any
    // if index fields are not set, pass empty array value
    val indexFields: Array[String] = options.get(INDEX_FIELDS_OPTION) match {
      case Some(value) => parseIndexFields(value)
      case None => Array.empty
    }
    log.info(s"Found optional index fields as ${indexFields.mkString("[", ", ", "]")}")
    val typeDesc = new TypeDescription(dataSchema, indexFields)
    log.info(s"Using type description $typeDesc to write output")

    // set job configuration based on provided Spark options, if not set - noop
    val conf = job.getConfiguration()
    // set compression codec, riff does not have default compression codec value, therefore
    // we just select one ourselves
    conf.set(Options.COMPRESSION_CODEC,
      sparkSession.conf.get(SQL_RIFF_COMPRESSION_CODEC, SQL_RIFF_COMPRESSION_CODEC_DEFAULT))

    // set stripe size
    conf.set(Options.STRIPE_ROWS,
      sparkSession.conf.get(SQL_RIFF_STRIPE_ROWS, s"${Options.STRIPE_ROWS_DEFAULT}"))

    // set column filters
    conf.set(Options.COLUMN_FILTER_ENABLED,
      sparkSession.conf.get(SQL_RIFF_COLUMN_FILTER_ENABLED,
        s"${Options.COLUMN_FILTER_ENABLED_DEFAULT}"))

    // set buffer size for outstream in riff
    conf.set(Options.BUFFER_SIZE,
      sparkSession.conf.get(SQL_RIFF_BUFFER_SIZE, s"${Options.BUFFER_SIZE_DEFAULT}"))

    val committerClass = classOf[RiffOutputCommitter]
    log.info(s"Using output committer for Riff: ${committerClass.getCanonicalName}")
    conf.setClass(SPARK_OUTPUT_COMMITTER_CLASS, committerClass, classOf[RiffOutputCommitter])

    new RiffOutputWriterFactory(typeDesc)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    None
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    // TODO: update file format to support split
    false
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    null
  }
}

object RiffFileFormat {
  // datasource option for fields to index; must be provided as comma-separated list of values
  val INDEX_FIELDS_OPTION = "index"

  // compression codec to use when writing riff files
  val SQL_RIFF_COMPRESSION_CODEC = "spark.sql.riff.compression.codec"
  // default codec for this file format
  val SQL_RIFF_COMPRESSION_CODEC_DEFAULT = "deflate"
  // number of rows per stripe to write
  val SQL_RIFF_STRIPE_ROWS = "spark.sql.riff.stripe.rows"
  // enable column filters for index fields
  val SQL_RIFF_COLUMN_FILTER_ENABLED = "spark.sql.riff.column.filter.enabled"
  // set buffer size in bytes for outstream
  val SQL_RIFF_BUFFER_SIZE = "spark.sql.riff.buffer.size"

  // internal Spark SQL option for output committer
  val SPARK_OUTPUT_COMMITTER_CLASS = "spark.sql.sources.outputCommitterClass"

  /**
   * Parse index fields string into list of indexed columns.
   * @param fields comma-separated list of field names that exist in data schema
   * @return array of index fields
   */
  def parseIndexFields(fields: String): Array[String] = {
    if (fields == null || fields.length == 0) {
      Array.empty
    } else {
      // remove empty field names
      fields.split(',').map(_.trim).filter(_.nonEmpty)
    }
  }
}
