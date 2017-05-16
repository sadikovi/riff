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

package com.github.sadikovi.hadoop.riff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.Metadata;
import com.github.sadikovi.riff.Riff;

/**
 * Riff output committer.
 * Provides methods to save metadata for the output folder.
 *
 * Note: Spark SQL does not use custom output committer when appending data to the existing table,
 * in our case it is okay, since we store only static information that does not change with writing
 * more data. In the future, if it is required to update metadata files, we will need to provide
 * custom commit protocol as subclass of `HadoopMapReduceCommitProtocol` through option
 * "spark.sql.sources.commitProtocolClass".
 */
public class RiffOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(RiffOutputCommitter.class);

  // root directory for write
  private final Path outputPath;

  public RiffOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    LOG.info("Commit job for path {}", outputPath);
    super.commitJob(jobContext);
    writeMetadataFile(jobContext.getConfiguration(), outputPath);
  }

  /**
   * Write Riff metadata file.
   * @param conf hadoop configuration
   * @param outputPath root path for output
   * @throws IOException
   */
  private static void writeMetadataFile(Configuration conf, Path outputPath) throws IOException {
    // Right now, merging of the schema is not supported as we do not support schema evolution or
    // merge. At this point, just find the first file of riff format and write metadata for it.
    FileSystem fs = outputPath.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(outputPath);
    List<FileStatus> partFiles = listFiles(fs, status, true);
    if (partFiles.isEmpty()) {
      LOG.warn("Could not find any part files for path {}, metadata is ignored", outputPath);
    } else {
      Metadata.MetadataWriter writer = Riff.metadataWriter(fs, conf, partFiles.get(0).getPath());
      writer.writeMetadataFile(outputPath);
      LOG.info("Finished writing metadata file for {}", outputPath);
    }
  }

  /**
   * Filter to read only part files, and discard any files that start with "_" or ".".
   */
  static class PartFileFilter implements PathFilter {
    public static final PartFileFilter instance = new PartFileFilter();

    private PartFileFilter() {}

    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith("_") && !path.getName().startsWith(".");
    }
  }

  /**
   * List all part files recursively, or return the first file found.
   * @param fs file system
   * @param fileStatus current directory file status or file
   * @param fetchOneFile whether or not to return just first found file or traverse fully
   * @return list of file statuses for part files
   */
  protected static List<FileStatus> listFiles(
      FileSystem fs,
      FileStatus fileStatus,
      boolean fetchOneFile) throws IOException {
    List<FileStatus> files = new ArrayList<FileStatus>();
    recurListFiles(fs, fileStatus, files, fetchOneFile);
    return files;
  }

  private static void recurListFiles(
      FileSystem fs,
      FileStatus fileStatus,
      List<FileStatus> foundFiles,
      boolean fetchOneFile) throws IOException {
    if (fetchOneFile && !foundFiles.isEmpty()) return;
    if (fileStatus.isDirectory()) {
      FileStatus[] list = fs.listStatus(fileStatus.getPath(), PartFileFilter.instance);
      for (int i = 0; i < list.length; i++) {
        recurListFiles(fs, list[i], foundFiles, fetchOneFile);
      }
    } else {
      // file status is a file, add to the list
      foundFiles.add(fileStatus);
    }
  }
}
