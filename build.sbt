organization := "org.jfratzke"

name := "spark-ramblings"

version := "1.4.21-spark-2.3.1"

scalaVersion := "2.11.12"

resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

publishTo := Some( "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository")

lazy val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.scalaj" %% "scalaj-http" % "2.4.1"
)

