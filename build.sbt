ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "SparkAssignment"
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
