ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "com.example"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "databricks_jar_test",
    idePackagePrefix := Some("example"),
    assembly / mainClass := Some("example.MyJob6"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "com.databricks" %% "dbutils-api" % "0.0.6" % "provided"
    )
  )
