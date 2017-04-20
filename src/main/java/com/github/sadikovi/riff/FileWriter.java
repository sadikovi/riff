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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.catalyst.InternalRow;

class FileWriter {
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

  /**
   * Create file writer for path.
   * @param fs file system to use
   * @param path path to the header file, data path is created from it
   * @throws IOException
   * @throws FileAlreadyExistsException
   */
  FileWriter(FileSystem fs, Path path, TypeDescription td) throws IOException {
    this.fs = fs;
    this.headerPath = fs.makeQualified(path);
    this.dataPath = makeDataPath(headerPath);
    this.td = td;

    if (this.fs.exists(headerPath)) {
      throw new FileAlreadyExistsException("Already exists: " + headerPath);
    }
    if (this.fs.exists(dataPath)) {
      throw new FileAlreadyExistsException(
        "Data path already exists: " + dataPath + ". Data path is created from provided path " +
        path + " and also should not exist when " + "creating writer");
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

  public void writeRows(Iterator<InternalRow> iter) {
    // TODO: implement writing rows
  }

  @Override
  public String toString() {
    return "FileWriter[header=" + headerPath + ", data=" + dataPath + ", type_desc=" + td + "]";
  }
}
