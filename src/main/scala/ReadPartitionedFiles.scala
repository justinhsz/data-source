package com.justinhsz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.sql.Date

object ReadPartitionedFiles extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val df = spark.read.format("com.justinhsz.source.partition.csv.CsvSource")
    .load("./csv_partitioned")
    .where(col("file_date") === Date.valueOf("2020-11-01"))

  df.explain(true)
  println(s"total rows: ${df.count()}")
}
