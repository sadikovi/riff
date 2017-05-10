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

package com.github.sadikovi.hadoop.riff

import org.apache.hadoop.fs.Path

import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class RiffOutputCommitterSuite extends UnitTestSuite {
  test("accept path in PartFileFilter") {
    import RiffOutputCommitter._
    // here we test what files part file filter discards
    PartFileFilter.instance.accept(new Path("/part-00000")) should be (true)
    PartFileFilter.instance.accept(new Path("/part-00000.crc")) should be (true)
    PartFileFilter.instance.accept(new Path("/_part-00000")) should be (false)
    PartFileFilter.instance.accept(new Path("/_metadata")) should be (false)
    PartFileFilter.instance.accept(new Path("/.part-00000.crc")) should be (false)
    // make sure that we filter out Riff temporary data files
    PartFileFilter.instance.accept(new Path("/part-00000.riff")) should be (true)
    PartFileFilter.instance.accept(new Path("/part-00000.riff.data")) should be (false)
  }

  test("list files and fetch first found") {
    withTempDir { dir =>
      // create directory tree with partitioning structure
      touch(dir / "col1=a" / "col2=1" / "part-00")
      touch(dir / "col1=a" / "col2=1" / ".part-00.crc")
      touch(dir / "col1=a" / "col2=2" / "part-01")
      touch(dir / "col1=a" / "col2=2" / ".part-01.crc")
      touch(dir / "col1=b" / "col2=1" / "part-02")
      touch(dir / "col1=b" / "col2=1" / ".part-02.crc")
      touch(dir / "col1=b" / "col2=2" / "part-03")
      touch(dir / "col1=b" / "col2=2" / ".part-03.crc")
      val res = RiffOutputCommitter.listFiles(fs, fs.getFileStatus(dir), true)
      res.size should be (1)
      assert(res.get(0).getPath.getName.startsWith("part-"))
    }
  }

  test("list files and fetch all files") {
    withTempDir { dir =>
      // create directory tree with partitioning structure
      touch(dir / "col1=a" / "col2=1" / "part-00")
      touch(dir / "col1=a" / "col2=1" / ".part-00.crc")
      touch(dir / "col1=a" / "col2=2" / "part-01")
      touch(dir / "col1=a" / "col2=2" / ".part-01.crc")
      touch(dir / "col1=b" / "col2=1" / "part-02")
      touch(dir / "col1=b" / "col2=1" / ".part-02.crc")
      touch(dir / "col1=b" / "col2=2" / "part-03")
      touch(dir / "col1=b" / "col2=2" / ".part-03.crc")
      val res = RiffOutputCommitter.listFiles(fs, fs.getFileStatus(dir), false)
      res.size should be (4)
      for (i <- 0 until res.size) {
        assert(res.get(i).getPath.getName.startsWith("part-"))
      }
    }
  }

  test("return empty list if no files exist") {
    withTempDir { dir =>
      // create directory tree with partitioning structure
      mkdirs(dir / "col1=a" / "col2=1")
      mkdirs(dir / "col1=a" / "col2=2")
      mkdirs(dir / "col1=b" / "col2=1")
      mkdirs(dir / "col1=b" / "col2=2")
      val res1 = RiffOutputCommitter.listFiles(fs, fs.getFileStatus(dir), true)
      res1.size should be (0)
      val res2 = RiffOutputCommitter.listFiles(fs, fs.getFileStatus(dir), false)
      res2.size should be (0)
    }
  }

  test("return root path when it is a file") {
    withTempDir { dir =>
      touch(dir / "file")
      val status = fs.getFileStatus(dir / "file")
      val res1 = RiffOutputCommitter.listFiles(fs, status, true)
      res1.size should be (1)
      res1.get(0) should be (status)
      val res2 = RiffOutputCommitter.listFiles(fs, status, false)
      res2.size should be (1)
      res2.get(0) should be (status)
    }
  }
}
