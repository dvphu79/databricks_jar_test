ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

ThisBuild / organization := "com.example"

lazy val root = (project in file("."))
  .settings(
    name := "databricks_jar_test",
    idePackagePrefix := Some("example")
  )
