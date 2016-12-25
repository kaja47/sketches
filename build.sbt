organization := "atrox"

name := "sketches"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "latest.integration" % "provided",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
)

javaOptions ++= Seq(
  "-Xmx12G", "-Xms6G"//, "-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading"
)
