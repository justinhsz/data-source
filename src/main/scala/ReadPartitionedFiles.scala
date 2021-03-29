package com.justinhsz

import org.apache.spark.sql.{Encoders, SparkSession}
import com.justinhsz.partitioned

object ReadPartitionedFiles extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val df = spark.read.format("com.justinhsz.partitioned.PartitionedFileSource")
    .option("fileExtension", "avro")
    .load("./covid-19-partitioned")

  df.show()
  println(s"total rows: ${df.count()}")
}
