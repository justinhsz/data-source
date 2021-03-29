package com.justinhsz

import org.apache.spark.sql.{Encoders, SparkSession}

case class Record(id: Int, name: String)

object Command extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val rdd = spark.sparkContext.parallelize(Seq(Record(1, "Pete")))
  spark.createDataset(rdd)(Encoders.product[Record]).show()
}
