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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * [[Metadata]] stores common information about Riff files in the directory or metadata for a single
 * file depending on an output path.
 */
public class Metadata {
  // file name for metadata files
  public static final String METADATA_FILENAME = "_riff_metadata";

  // type description for this metadata
  private TypeDescription typeDescription;

  /**
   * Initialize new instance of metadata with following attributes.
   * @param td type description
   */
  protected Metadata(TypeDescription td) {
    this.typeDescription = td;
  }

  protected Metadata() {
    this.typeDescription = null;
  }

  /**
   * Get type description.
   * If metadata is not initialized correctly, returned value is null.
   * @return type description for metadata
   */
  public TypeDescription getTypeDescription() {
    return this.typeDescription;
  }

  /**
   * Write this metadata into external output stream.
   * @param out output stream
   * @throws IOException
   */
  protected void writeTo(OutputStream out) throws IOException {
    typeDescription.writeTo(out);
  }

  /**
   * Read metadata from external stream.
   * @param in input stream
   * @throws IOException
   */
  protected void readFrom(InputStream in) throws IOException {
    typeDescription = TypeDescription.readFrom(in);
  }


  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + typeDescription + ")";
  }

  public static class MetadataWriter {
    // file system to use
    private final FileSystem fs;
    // metadata for this writer (kept internally for subsequent write)
    private final Metadata metadata;

    /**
     * Create instance of metadata wrtier for a Riff file.
     * @param fs file system to use
     * @param conf hadoop configuration with riff settings
     * @param filepath Riff filepath to a valid header file
     */
    public MetadataWriter(FileSystem fs, Configuration conf, Path filepath) throws IOException {
      // infer metadata path and read header file
      FileReader reader = new FileReader(fs, conf, filepath);
      this.fs = fs;
      this.metadata = new Metadata(reader.readFileHeader().getTypeDescription());
      reader = null;
    }

    /**
     * Write metadata file in output location. If output location is directory - file is created in
     * that directory, otherwise file is created at the same level as outputPath file.
     * @param outputPath path to the output directory or file
     */
    public void writeMetadataFile(Path outputPath) throws IOException {
      FileStatus status = fs.getFileStatus(outputPath);
      Path parent = status.isDirectory() ? status.getPath() : status.getPath().getParent();
      FSDataOutputStream out = null;
      try {
        Path metadataPath = new Path(parent, METADATA_FILENAME);
        out = fs.create(metadataPath, false);
        metadata.writeTo(out);
      } finally {
        if (out != null) {
          out.close();
        }
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  public static class MetadataReader {
    // file system to use
    private final FileSystem fs;
    // path to the metadata file
    private final Path metadataPath;

    /**
     * Create instance of metadata reader.
     * @param fs file system
     * @param conf hadoop configuration with riff settings
     * @param metadataPath path to the metadata file or directory where metadata file exists
     */
    public MetadataReader(FileSystem fs, Configuration conf, Path metadataPath) {
      this.fs = fs;
      this.metadataPath = metadataPath;
    }

    /**
     * Read metadata file and return deserialized metadata.
     * @param ignoreNotFound when true, ignore read and return null if metadata is not found
     * @return metadata instance of null if not found
     * @throws IOException
     */
    public Metadata readMetadataFile(boolean ignoreNotFound) throws IOException {
      // if `ignoreNotFound` flag is set to true, we ignore errors associated with locating
      // metadata file, e.g. when directory of part files does not contain metadata
      if (!fs.exists(metadataPath) && ignoreNotFound) {
        return null;
      }
      FileStatus status = fs.getFileStatus(metadataPath);
      Path parent = status.isDirectory() ? status.getPath() : status.getPath().getParent();
      Path metadataPath = new Path(parent, METADATA_FILENAME);
      if (!fs.exists(metadataPath) && ignoreNotFound) {
        return null;
      }
      // otherwise create input stream and read metadata
      FSDataInputStream in = null;
      try {
        Metadata metadata = new Metadata();
        in = fs.open(metadataPath);
        metadata.readFrom(in);
        return metadata;
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }
}
