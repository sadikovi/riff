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

package com.github.sadikovi.riff;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.catalyst.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.OutputBuffer;

class FileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);
  // suffix for data files
  private static final String DATA_FILE_SUFFIX = ".data";

  // file system to use for writing a file
  private FileSystem fs;
  // resolved path for header file
  private Path headerPath;
  // resolved path for data file
  private Path dataPath;
  // type description for schema to write
  private TypeDescription td;
  // unique id for this writer, assigned per file
  private byte[] fileId;
  // whether or not this file writer has created file already
  private boolean hasWrittenData;
  // number of rows in stripe
  private int numRowsInStripe;

  /**
   * Create file writer for path.
   * Configuration is passed separately and not reused from `fs.getConf`. This is to be explicit
   * about separate configuration from most of the hadoop settings. Actual user-facing API will
   * allow providing configuration for both file system and internal options.
   * @param fs file system to use
   * @param conf configuration
   * @param path path to the header file, also used to create data path
   * @param td type description for rows
   * @throws IOException
   * @throws FileAlreadyExistsException
   */
  FileWriter(FileSystem fs, Configuration conf, Path path, TypeDescription td) throws IOException {
    this.fs = fs;
    this.headerPath = fs.makeQualified(path);
    this.dataPath = makeDataPath(headerPath);
    this.hasWrittenData = false;
    if (this.fs.exists(headerPath)) {
      throw new FileAlreadyExistsException("Already exists: " + headerPath);
    }
    if (this.fs.exists(dataPath)) {
      throw new FileAlreadyExistsException(
        "Data path already exists: " + dataPath + ". Data path is created from provided path " +
        path + " and also should not exist when " + "creating writer");
    }
    // this assumes that subsequent rows are provided for this schema
    this.td = td;
    // generate unique id for this file, collisions are possible, this mainly to prevent accidental
    // renaming of files
    this.fileId = nextFileKey();

    this.numRowsInStripe =
      conf.getInt(Riff.Options.RIFF_STRIPE_ROWS, Riff.Options.RIFF_STRIPE_ROWS_DEFAULT);
    if (numRowsInStripe < 1) {
      throw new IllegalArgumentException(
        "Expected positive number of rows in stripe, found " + numRowsInStripe);
    }
  }

  /**
   * Append data file suffix to the path, suffix is always the last block in file name.
   * @param path header path
   * @return data path
   */
  static Path makeDataPath(Path path) {
    return path.suffix(DATA_FILE_SUFFIX);
  }

  /**
   * Generate new file key of 12 bytes for file identifier.
   * This key is shared between header and data files.
   * @return array with random byte values
   */
  static byte[] nextFileKey() {
    Random rand = new Random();
    byte[] key = new byte[12];
    rand.nextBytes(key);
    return key;
  }

  /**
   * Return header path for writer.
   * @return header path
   */
  public Path headerPath() {
    return headerPath;
  }

  /**
   * Return data path for writer.
   * @return data path
   */
  public Path dataPath() {
    return dataPath;
  }

  /**
   * Create header and data files and write rows.
   * This method invokes collection of any statistics or filters relevant to the file and writes
   * stripes of data into file.
   * @param iter iterator of Spark internal rows
   */
  public void writeFile(Iterator<InternalRow> iter) throws IOException {
    // assert if this file writer has written data already, and fail if so - we do not allow to
    // reuse instances for multiple data writes.
    if (hasWrittenData) {
      throw new IOException("No reuse of file writer " + this);
    }
    hasWrittenData = true;

    // first, we write data file and collect all stripe information in order to store it in header
    // data header consists of 16 bytes - 4 bytes magic and 12 bytes unique key
    OutputBuffer dataHeader = new OutputBuffer();
    dataHeader.writeBytes(Riff.MAGIC.getBytes());
    dataHeader.writeBytes(fileId);
    assert dataHeader.bytesWritten() == 16: "Invalid number of bytes written - expected 16, got " +
      dataHeader.bytesWritten();
    // TODO: implement writing rows
  }

  @Override
  public String toString() {
    return "FileWriter[header=" + headerPath + ", data=" + dataPath + ", type_desc=" + td + "]";
  }
}
