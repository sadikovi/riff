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

package org.apache.spark.sql.riff

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.riff.{Riff, TypeDescription}
import com.github.sadikovi.riff.io.CompressionCodecFactory

import org.slf4j.LoggerFactory

/**
 * Riff output writer factory to create instances of output writer on each executor.
 * To support both 2.0 and 2.1 versions of Spark we provide 2 methods of `newInstance` which are
 * handled separately by output writer.
 */
class RiffOutputWriterFactory(val desc: TypeDescription) extends OutputWriterFactory {
  override def getFileExtension(context: TaskAttemptContext): String = {
    val conf = context.getConfiguration()
    CompressionCodecFactory.fileExtForShortName(conf.get(Riff.Options.COMPRESSION_CODEC)) + ".riff"
  }

  /**
   * Method for Spark 2.0.x, where it is required to provide bucket id.
   * We do not use `override` modifier since this method is removed in 2.1
   */
  def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    // legacy support for Spark 2.0.x
    val legacySupport = true
    new RiffOutputWriter(path, desc, context, getFileExtension(context), legacySupport, bucketId)
  }

  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    // bucket id has been removed in 2.1 and full path is sent to the writer
    val legacySupport = false
    new RiffOutputWriter(path, desc, context, getFileExtension(context), legacySupport)
  }
}

/**
 * Riff output writer for a single file, initialized on each executor.
 */
class RiffOutputWriter(
    path: String,
    typeDesc: TypeDescription,
    context: TaskAttemptContext,
    extension: String,
    legacySupport: Boolean,
    bucketId: Option[Int] = None)
  extends OutputWriter {

  @transient private val log = LoggerFactory.getLogger(classOf[RiffOutputWriter])

  // create writer using hadoop configuration for task attempt
  val configuration = context.getConfiguration()
  val filepath = if (legacySupport) {
    // we create file name manually taking into account bucket id (if provided)
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
    new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$bucketString.json$extension")
  } else {
    new Path(path)
  }
  // prepare riff writer, all options should be set through hadoop configuration
  val writer = Riff.writer
    .setTypeDesc(typeDesc)
    .setConf(configuration)
    .create(filepath)
  log.info(s"Using file writer $writer")
  writer.prepareWrite()

  override def write(row: Row): Unit = {
    // writer supports internal row writes only
    throw new UnsupportedOperationException("call writeInternal")
  }

  // this method is protected[sql] in Spark
  override def writeInternal(row: InternalRow): Unit = {
    writer.write(row)
  }

  override def close(): Unit = {
    writer.finishWrite()
  }
}
