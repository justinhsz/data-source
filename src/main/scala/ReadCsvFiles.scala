package com.justinhsz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ReadCsvFiles extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val df = spark.read
    .format("com.justinhsz.source.csv.CsvSource")
    .option("inferSchema", true)
    .load("./csv_partitioned")
  val usDF = df.where(
    col("Country_Region").equalTo("US")
  ).select("Province_State", "Confirmed", "Deaths", "Recovered")
  usDF.explain(true)

  usDF.show()
  println(s"total rows: ${usDF.count()}")

//  val defaultDf = spark.read.option("header", true).option("inferSchema", true).csv("./csv_partitioned")
//  defaultDf.show()
//  val usDF = defaultDf.where(col("month").geq(10) and col("Country_Region").equalTo("US")).select("Province_State", "Confirmed", "Deaths", "Recovered")
//  usDF.explain(true)
//
//  val count = usDF.count()
//  println(s"month 12 row count: $count")
}
