package com.justinhsz

import org.apache.spark.sql.{Encoders, SparkSession}
import com.justinhsz.source

object ReadPartitionedFiles extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val df = spark.read.format("com.justinhsz.source.csv.FileSource")
    .load("./covid-19-partitioned")

  println(s"total rows: ${df.count()}")
}
