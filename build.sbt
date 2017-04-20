name := "riff"

organization := "com.github.sadikovi"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "sadikovi/riff"

val defaultSparkVersion = "2.1.0"

sparkVersion := sys.props.getOrElse("spark.testVersion", defaultSparkVersion)

val defaultHadoopVersion = "2.6.0"

val hadoopVersion = settingKey[String]("The version of Hadoop to test against.")

hadoopVersion := sys.props.getOrElse("hadoop.testVersion", defaultHadoopVersion)

spAppendScalaVersion := true

spIncludeMaven := false

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.6.2.Final",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

// check deprecation without manual restart
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
// Exclude scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}
coverageMinimum := 80
coverageFailOnMinimum := true

EclipseKeys.eclipseOutput := Some("target/eclipse")

// tasks dependencies
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile).dependsOn(compileScalastyle)

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value
(test in Test) <<= (test in Test).dependsOn(testScalastyle)
