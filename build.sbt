name := "kafka2hive-batch"

version := "0.1"

ThisBuild / scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-walkthrough-common" % "1.13.2" % "compile",
  "org.apache.flink" %% "flink-streaming-scala" % "1.13.2" % "compile",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.12.1" % "compile",
  "org.apache.flink" %% "flink-clients" % "1.13.2" % "compile",
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1" % "compile"
) ++ Seq(
  "org.scalatest" %% "scalatest" % "3.2.3" % "test",
)
