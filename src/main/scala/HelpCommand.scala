package com.justinhsz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, month, split, to_date, udf, year}

object HelpCommand {
  def getSampleDataset() = {
    sys.runtime.exec("git clone https://github.com/CSSEGISandData/COVID-19.git")
  }

  def partitionDataset(master: Option[String]) = {
    val spark = SparkSession.builder().master(master.getOrElse("local")).getOrCreate()
    val name_only = udf{ filePath: String => filePath.split("/").last.takeWhile( _ != '.')}

    val fileDateColumn = to_date(name_only(input_file_name()), "MM-dd-yyyy")
    val df = spark.read
      .option("header", true)
      .csv("./COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/")
      .withColumn("year", year(fileDateColumn))
      .withColumn("month", month(fileDateColumn))

    df.show()
    df.write
      .option("header", true)
      .mode("overwrite")
      .partitionBy("year", "month")
      .csv("./covid-19-partitioned")
  }

  def main(args: Array[String]): Unit = {
    if(args.length == 0) {
      println("Do nothing")
    } else {
      args(0) match {
        case "download" => getSampleDataset()
        case "preprocess" => partitionDataset(None)
        case _ => println("Do nothing")
      }
    }
  }
}
