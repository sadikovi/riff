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

package com.github.sadikovi.serde

import java.util.UUID
import java.sql.Timestamp

import scala.sys.process._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.github.sadikovi.serde.io._
import com.github.sadikovi.testutil.UnitTestSuite
import com.github.sadikovi.testutil.implicits._

// Update test results after each major change
/*
== Performance test suite ==
Time: 2017-04-19 20:48:33.206
Commit: "74d533373c73fbfcc9b2c8d1e2d72f95a156c6f1"

== Write batch of rows ==
Writing of 100000 records took 320.259 ms
Created file of 20500000 bytes


== Write/read batch of rows ==
Processing of 100000 records took 280.194 ms
Data file has 20500000 bytes

== End of performance test suite ==
*/
class IndexedRowPerfSuite extends UnitTestSuite {
  def newRow(i: Int = 1): InternalRow = {
    InternalRow(i, i.toLong, i.toLong,
      UTF8String.fromString(UUID.randomUUID.toString),
      UTF8String.fromString(UUID.randomUUID.toString),
      UTF8String.fromString(UUID.randomUUID.toString),
      UTF8String.fromString(UUID.randomUUID.toString))
  }

  def schema: StructType = {
    StructType(
      StructField("col1", IntegerType) ::
      StructField("col2", LongType) ::
      StructField("col3", LongType) ::
      StructField("col4", StringType) ::
      StructField("col5", StringType) ::
      StructField("col6", StringType) ::
      StructField("col7", StringType) :: Nil)
  }

  override def beforeAll {
    println("== Performance test suite ==")
    println(s"Time: ${new Timestamp(System.currentTimeMillis)}")
    println(s"Commit: ${"""git log --pretty=format:"%H" -n1""".!!.trim}")
  }

  override def afterAll {
    println("== End of performance test suite ==")
  }

  test("write uncompressed batch of rows") {
    withTempDir { dir =>
      val numRecords = 100000
      val rows = (0 until numRecords).map { i => newRow(i) }
      val startTime = System.nanoTime

      // == begin write records into stream
      val td = new TypeDescription(schema, Array("col2", "col5"))
      val stripe = new StripeOutputBuffer(1.toByte)
      val outStream = new OutStream(128, null, stripe)
      val writer = new IndexedRowWriter(td)
      val out = create(dir / "file")
      try {
        for (row <- rows) {
          writer.writeRow(row, outStream)
        }
        outStream.flush()
        stripe.flush(out)
      } finally {
        out.close()
      }
      // == end write records into stream

      val endTime = System.nanoTime
      val fileSize = fs.getFileStatus(dir / "file").getLen
      println()
      println("== Write batch of rows ==")
      println(s"Writing of $numRecords records took ${(endTime - startTime) / 1e6} ms")
      println(s"Created file of $fileSize bytes")
      println()
    }
  }

  test("write/read uncompressed stripe of rows") {
    withTempDir { dir =>
      val numRecords = 100000
      val rows = (0 until numRecords).map { i => newRow(i) }
      val startTime = System.nanoTime

      // == begin write records into stream
      val td = new TypeDescription(schema, Array("col2", "col5"))
      val outStripe = new StripeOutputBuffer(1.toByte)
      val outStream = new OutStream(128, null, outStripe)
      val writer = new IndexedRowWriter(td)
      val out = create(dir / "file")
      try {
        for (row <- rows) {
          writer.writeRow(row, outStream)
        }
        outStream.flush()
        outStripe.flush(out)
      } finally {
        out.close()
      }

      val reader = new IndexedRowReader(td)
      val in = open(dir / "file")
      try {
        val inStripe = new StripeInputBuffer(1.toByte, outStripe.array())
        val inStream = new InStream(128, null, inStripe)
        while (inStream.available() != 0) {
          reader.readRow(inStream)
        }
      } finally {
        in.close()
      }
      // == end write records into stream

      val endTime = System.nanoTime
      val fileSize = fs.getFileStatus(dir / "file").getLen
      println()
      println("== Write/read batch of rows ==")
      println(s"Processing of $numRecords records took ${(endTime - startTime) / 1e6} ms")
      println(s"Data file has $fileSize bytes")
      println()
    }
  }
}
