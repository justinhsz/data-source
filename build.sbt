name := "spark-data-source"

version := "0.1"

scalaVersion := "2.11.8"

idePackagePrefix := Some("com.justinhsz")

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql"   % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)