ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.5.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sketch" % "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "bloomFiltering"
  )
