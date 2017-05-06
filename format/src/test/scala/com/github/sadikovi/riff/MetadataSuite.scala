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

package com.github.sadikovi.riff

import java.io.IOException

import org.apache.spark.sql.types._

import com.github.sadikovi.testutil.implicits._
import com.github.sadikovi.testutil.UnitTestSuite

class MetadataSuite extends UnitTestSuite {
  test("write metadata for a file with provided file path as output") {
    withTempDir { dir =>
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val writer = Riff.writer.setTypeDesc(schema).create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      // create metadata for a file
      val mwriter = Riff.metadataWriter.create(dir / "file")
      mwriter.writeMetadataFile(dir / "file")

      fs.exists(dir / Metadata.METADATA_FILENAME) should be (true)
    }
  }

  test("write metadata for a file with provided file directory as output") {
    withTempDir { dir =>
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val writer = Riff.writer.setTypeDesc(schema).create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      // create metadata for a file
      val mwriter = Riff.metadataWriter.create(dir / "file")
      mwriter.writeMetadataFile(dir)

      fs.exists(dir / Metadata.METADATA_FILENAME) should be (true)
    }
  }

  test("write/read metadata for a file") {
    withTempDir { dir =>
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val writer = Riff.writer.setTypeDesc(schema).create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      // create metadata for a file
      val mwriter = Riff.metadataWriter.create(dir / "file")
      mwriter.writeMetadataFile(dir)

      val mreader = Riff.metadataReader.create(dir)
      val metadata = mreader.readMetadataFile(false)

      metadata.getTypeDescription should be (new TypeDescription(schema))
    }
  }

  test("write/read metadata for a file, direct path") {
    withTempDir { dir =>
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val writer = Riff.writer.setTypeDesc(schema).create(dir / "file")
      writer.prepareWrite()
      writer.finishWrite()
      // create metadata for a file
      val mwriter = Riff.metadataWriter.create(dir / "file")
      mwriter.writeMetadataFile(dir)

      val mreader = Riff.metadataReader.create(dir / Metadata.METADATA_FILENAME)
      val metadata = mreader.readMetadataFile(false)

      metadata.getTypeDescription should be (new TypeDescription(schema))
    }
  }

  test("ignore metadata if file is not found in directory") {
    withTempDir { dir =>
      val mreader = Riff.metadataReader.create(dir)
      val metadata = mreader.readMetadataFile(true)
      metadata should be (null)
    }
  }

  test("ignore metadata if file is not found, direct path") {
    withTempDir { dir =>
      val mreader = Riff.metadataReader.create(dir / Metadata.METADATA_FILENAME)
      val metadata = mreader.readMetadataFile(true)
      metadata should be (null)
    }
  }

  test("fail to read metadata if file is not found and ignoreNotFound is false") {
    withTempDir { dir =>
      val mreader = Riff.metadataReader.create(dir / Metadata.METADATA_FILENAME)
      val err = intercept[IOException] {
        mreader.readMetadataFile(false)
      }
      assert(err.getMessage.contains("does not exist"))
    }
  }
}
