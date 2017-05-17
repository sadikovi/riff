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
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sadikovi.riff.io.ByteBufferStream;
import com.github.sadikovi.riff.io.OutputBuffer;

/**
 * Header information for Riff file.
 * All format checks should be done here.
 */
public class FileHeader {
  private static final Logger LOG = LoggerFactory.getLogger(FileHeader.class);
  // magic for Riff file, "RIFF" bytes in UTF8 charset
  private static final int MAGIC = 1380533830;
  // state length in bytes
  private static final int STATE_LENGTH = 8;

  // byte array of file state
  private final byte[] state;
  // type description for file
  private final TypeDescription td;
  // file statistics
  private final Statistics[] fileStats;

  /**
   * Initialize file header with state, type description and file statisitcs.
   * @param state file state
   * @param td file type description
   * @param fileStats file statistics
   */
  public FileHeader(byte[] state, TypeDescription td, Statistics[] fileStats) {
    if (state.length != STATE_LENGTH) {
      throw new IllegalArgumentException("Invalid state length, " +
        state.length + " != " + STATE_LENGTH);
    }
    this.state = state;
    this.td = td;
    this.fileStats = fileStats;
  }

  /**
   * Initialize file header with type description and file statisitcs.
   * State can be modified using `setState` method.
   * @param td type description
   * @param fileStats file statistics
   */
  public FileHeader(TypeDescription td, Statistics[] fileStats) {
    this(new byte[STATE_LENGTH], td, fileStats);
  }

  /**
   * Set state for position in byte array.
   * @param pos position in array
   * @param flag value to set
   */
  public void setState(int pos, byte flag) {
    state[pos] = flag;
  }

  /**
   * Get type description.
   * @return type description
   */
  public TypeDescription getTypeDescription() {
    return td;
  }

  /**
   * Get file statistics.
   * @return list of file statistics
   */
  public Statistics[] getFileStatistics() {
    return fileStats;
  }

  /**
   * Get state flag for posiiton.
   * @param pos position of the flag
   * @return state value
   */
  public byte state(int pos) {
    return state[pos];
  }

  /**
   * Write header into output stream.
   * Stream is not closed after this operation is complete.
   * @param out output stream
   * @throws IOException
   */
  public void writeTo(FSDataOutputStream out) throws IOException {
    OutputBuffer buffer = new OutputBuffer();
    // record file header
    buffer.write(state);
    td.writeTo(buffer);
    buffer.writeInt(fileStats.length);
    int i = 0;
    while (i < fileStats.length) {
      LOG.debug("Write file statistics {}", fileStats[i]);
      fileStats[i].writeExternal(buffer);
      ++i;
    }
    buffer.align();
    LOG.info("Write header content of {} bytes", buffer.bytesWritten());
    // write magic 4 bytes + buffer length 4 bytes into output stream
    out.writeLong(((long) MAGIC << 32) + buffer.bytesWritten());
    // write buffer data
    buffer.writeExternal(out);
  }

  /**
   * Read header from input stream.
   * Stream is not closed after operation is complete.
   * @param in input stream
   * @throws IOException
   */
  public static FileHeader readFrom(FSDataInputStream in) throws IOException {
    // Read first 8 bytes: magic 4 bytes and length of the header 4 bytes
    long meta = in.readLong();
    int magic = (int) (meta >>> 32);
    if (magic != MAGIC) throw new IOException("Wrong magic: " + magic + " != " + MAGIC);
    int len = (int) (meta & 0x7fffffff);
    LOG.debug("Read header content of {} bytes", len);
    // read full header bytes
    ByteBuffer buffer = ByteBuffer.allocate(len);
    in.readFully(buffer.array(), buffer.arrayOffset(), buffer.limit());
    // no flip - we have not reset position
    // read byte state
    byte[] state = new byte[STATE_LENGTH];
    buffer.get(state);
    // read type description
    // currently type description supports serde from stream, therefore we create stream that
    // wraps byte buffer, stream updates position of buffer
    ByteBufferStream byteStream = new ByteBufferStream(buffer);
    TypeDescription td = TypeDescription.readFrom(byteStream);
    // read file statistics
    Statistics[] fileStats = new Statistics[buffer.getInt()];
    int i = 0;
    while (i < fileStats.length) {
      fileStats[i] = Statistics.readExternal(buffer);
      LOG.debug("Read file statistics {}", fileStats[i]);
      ++i;
    }
    return new FileHeader(state, td, fileStats);
  }
}
