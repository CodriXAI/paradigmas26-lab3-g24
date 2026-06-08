name := "reddit-ner-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

fork := true

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  // Allows access to internal Java classes required for Spark 3.x with Java 17+
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "org.apache.spark" %% "spark-sql" % "3.5.1"
)
