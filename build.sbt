ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "com.example"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "databricks_jar_test",
    idePackagePrefix := Some("example"),
    compile / mainClass := Some("example.MyJob"),
    assembly / mainClass := Some("example.MyJob"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.databricks" %% "dbutils-api" % "0.0.6"
    ),
  )