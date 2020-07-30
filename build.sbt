name := "avro-unions-and-default-values"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies := Seq(
  "org.apache.avro" % "avro" % "1.10.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)