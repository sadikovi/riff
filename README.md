# riff
Spark SQL row-oriented file format

[![Build Status](https://travis-ci.org/sadikovi/riff.svg?branch=master)](https://travis-ci.org/sadikovi/riff)

Riff is a Spark SQL row-oriented file format designed for faster writes and point queries, and
reasonable range queries compare to Parquet. It is built to work _natively_ with Spark SQL
eliminating row values conversion, performing predicate pushdown and indexing. You can see benchmark
results in [this gist](https://gist.github.com/sadikovi/41c07e9f76177820b7f9894c79a2efa1).

## Requirements
| Spark version | riff latest version |
|---------------|------------------------------|
| 2.0.x | 0.1.0 (not published yet, [build from the source](#building-from-source)) |
| 2.1.x | 0.1.0 (not published yet, [build from the source](#building-from-source)) |

## Linking
The `riff` package can be added to Spark by using the `--jars` command line option.
For example, run this to include it when starting `spark-shell` (Scala 2.11.x):
```shell
 $SPARK_HOME/bin/spark-shell --jars ./target/scala-2.11/riff_2.11-0.1.0-SNAPSHOT.jar
```
See build instructions to create jar for Scala 2.10.x

## Spark options
Currently supported options, use `--conf key=value` on a command line to provide options similar to
other Spark configuration or add them to `spark-defaults.conf` file, or add them in the code by
running `spark.conf.set("option", "value")`.

| Name | Description | Default |
|------|-------------|---------|
| `spark.sql.riff.compression.codec` | Compression codec to use for riff (`none`, `snappy`, `gzip`, `deflate`) | `deflate`
| `spark.sql.riff.stripe.rows` | Number of rows to keep per stripe | `10000`
| `spark.sql.riff.column.filter.enabled` | When enabled, write column filters in addition to min/max/null statistics (`true`, `false`) | `true`
| `spark.sql.riff.buffer.size` | Buffer size in bytes for out/in stream | `256 * 1024`
| `spark.sql.riff.filterPushdown` | When enabled, propagate filter to riff format, otherwise filter data in Spark only | `true`

## DataFrame options
These options that you can specify when writing DataFrame by calling `df.write.option("key", "value").save(...)`.

| Name | Description | Default |
|------|-------------|---------|
| `index` | Optional setting to specify columns to index by Riff; if no columns provided, default row layout is used | `<empty string>`

## Supported Spark SQL types
- `IntegerType`
- `LongType`
- `StringType`
- `DateType`
- `TimestampType`
- `BooleanType`
- `ShortType`
- `ByteType`

## Example
Usage is very similar to other datasources for Spark, e.g. Parquet, ORC, JSON, etc. Riff allows to
set some datasource options in addition to ones listed in the table above.

### Scala API
```scala
// Write DataFrame into Riff format by using standard specification
val df: DataFrame = ...
df.write.format("com.github.sadikovi.spark.riff").save("/path/to/table")

// Read DataFrame from Riff format by using standard specification
val df = spark.read.format("com.github.sadikovi.spark.riff").load("/path/to/table")
```

Alternatively you can use shortcuts to write and read Riff files.
```scala
// You can also import implicit conversions to make it similar to Parquet read/write
import com.github.sadikovi.spark.riff._
val df: DataFrame = ...
df.write.riff("/path/to/table")

val df = spark.read.riff("/path/to/table")

// You can specify fields to index, Riff will create column filters for those, and restructure
// records to optimize filtering by those fields. This is optional and can be specified on writes,
// when reading data Riff will automatically use those fields - no settings required.
val df: DataFrame = ...
df.write.option("index", "col1,col2,col3").riff("/path/to/table")

val df = spark.read.riff("/path/to/table").filter("col1 = 'abc'")
```

### Python API
```python
# You can also specify index columns for table
df.write.format("com.github.sadikovi.spark.riff").save("/path/to/table")
...
df = spark.read.format("com.github.sadikovi.spark.riff").load("/path/to/table")
```

### SQL API
```sql
CREATE TEMPORARY VIEW test
USING com.github.sadikovi.spark.riff
OPTIONS (path "/path/to/table");

SELECT * FROM test LIMIT 10;
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.
To build jars for Scala 2.10.x and 2.11.x run `sbt +package`.

## Testing
Run `sbt test` from project root. See `.travis.yml` for CI build matrix.

## Running benchmark

Run `sbt package` to package project, next run `spark-submit` for following benchmarks. All data
created during benchmarks is stored in `./temp` folder.

- Write benchmark
```
spark-submit --class com.github.sadikovi.benchmark.WriteBenchmark \
  target/scala-2.11/riff_2.11-0.1.0-SNAPSHOT.jar
```

- Scan benchmark
```
spark-submit --class com.github.sadikovi.benchmark.ScanBenchmark \
  target/scala-2.11/riff_2.11-0.1.0-SNAPSHOT.jar
```

- Query benchmark
```
spark-submit --class com.github.sadikovi.benchmark.QueryBenchmark \
  target/scala-2.11/riff_2.11-0.1.0-SNAPSHOT.jar
```

- Project benchmark
```
spark-submit --class com.github.sadikovi.benchmark.ProjectBenchmark \
  target/scala-2.11/riff_2.11-0.1.0-SNAPSHOT.jar
```
