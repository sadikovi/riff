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

import java.net.URI

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.riff._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{And, DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

import com.github.sadikovi.riff.{Buffers, Riff}
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
  // type description for this source, when inferring schema
  private var typeDescription: TypeDescription = null

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
    // when inferring schema we expect at least one data file in directory
    if (files.isEmpty) {
      log.warn("No paths found for schema inferrence")
      None
    } else {
      // Spark, when returning parsed file paths, does not include metadata files, only files that
      // are either partitioned or do not begin with "_". This forces us to reconstruct metadata path
      // manually.

      // Logic is to use "path" key set in parameters map; this is set by DataFrameReader and
      // contains raw unresolved path to the directory or file. We make path qualified and check
      // if it contains metadata, otherwise fall back to reading single file from the list.

      // Normally "path" key contains single path, we apply the same logic as Spark datasource
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      val metadata = parameters.get("path").map { path =>
        val hdfsPath = new Path(path)
        val fs = hdfsPath.getFileSystem(hadoopConf)
        val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
        val reader = Riff.metadataReader.setFileSystem(fs).setConf(hadoopConf).create(qualifiedPath)
        reader.readMetadataFile(true)
      }

      // metadata can be null, we need to check
      metadata match {
        case Some(value) if value != null =>
          log.info("Infer schema from metadata")
          typeDescription = value.getTypeDescription
        case other =>
          log.info("Failed to load metadata, infer schema from header file")
          // infer schema from the first file in the list
          val headerFileStatus: Option[FileStatus] = files
            .filter(!_.getPath.getName.endsWith(Riff.DATA_FILE_SUFFIX))
            .headOption
          if (headerFileStatus.isEmpty) {
            throw new RuntimeException(s"No header files found in list ${files.toList}")
          }
          val headerFile = headerFileStatus.get.getPath
          val fs = headerFile.getFileSystem(hadoopConf)
          val reader = Riff.reader.setFileSystem(fs).setConf(hadoopConf).create(headerFile)
          typeDescription = reader.readTypeDescription()
      }
      Option(typeDescription).map(_.toStructType)
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    // TODO: update file format to support split
    false
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // right now Spark appends partition values to each row, it would be good to try doing it
    // internally somehow

    // check if filter pushdown disabled
    val filterPushdownEnabled = sparkSession.conf.get(SQL_RIFF_FILTER_PUSHDOWN,
      SQL_RIFF_FILTER_PUSHDOWN_DEFAULT).toBoolean

    val predicate = if (filterPushdownEnabled) {
      val reducedFilter = filters.reduceOption(And)
      val tree = Filters.createRiffFilter(reducedFilter)
      if (tree != null) {
        log.info(s"Applying filter tree $tree")
      }
      tree
    } else {
      log.info("Filter pushdown disabled")
      null
    }

    // set buffer size for instream in riff
    hadoopConf.set(Options.BUFFER_SIZE,
      sparkSession.conf.get(SQL_RIFF_BUFFER_SIZE, s"${Options.BUFFER_SIZE_DEFAULT}"))

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // right now, riff does not support column pruning, hence we ignore required schema and just
    // return iterator of rows
    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)
      // because riff stores header and data files separately, we have to ignore data files
      if (file.filePath.endsWith(Riff.DATA_FILE_SUFFIX)) {
        Buffers.emptyRowBuffer().asScala
      } else {
        val path = new Path(new URI(file.filePath))
        val hadoopConf = broadcastedHadoopConf.value.value
        val reader = Riff.reader.setConf(hadoopConf).create(path)
        val iter = reader.prepareRead(predicate)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
        iter.asScala
      }
    }
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
  // enable/disable filter pushdown for the format
  val SQL_RIFF_FILTER_PUSHDOWN = "spark.sql.riff.filterPushdown"
  val SQL_RIFF_FILTER_PUSHDOWN_DEFAULT = "true"

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
