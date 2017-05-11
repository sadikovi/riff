# riff
Spark SQL row serde and file format

[![Build Status](https://travis-ci.org/sadikovi/riff.svg?branch=master)](https://travis-ci.org/sadikovi/riff)

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
