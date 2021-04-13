package com.justinhsz
package source.csv

import source.CustomizedConversion._
import source.Utils.fs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util
import scala.collection.JavaConversions._

class CsvDataSourceReader(options: DataSourceOptions) extends DataSourceReader {
  private val rawPath = options.get("path")
  require(rawPath.isPresent, "require giving the path.")

  private val paths = rawPath.get().split(",").map(new Path(_))

  private lazy val fileList = fs.listStatus(paths).toList.flatMap { fileOrDir =>
    if (fileOrDir.isDirectory) {
      fs.listFiles(fileOrDir.getPath, true)
    } else {
      Iterator(fileOrDir)
    }
  }.filter(_.getPath.getName.toLowerCase.endsWith(".csv"))

  override def readSchema(): StructType = {
    val reader = new CsvReader(fileList.head.getPath.toString)
    val schema = StructType(reader.fileFields.map { field => StructField(field, StringType) })
    reader.close()
    schema
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val schema = readSchema()
    fileList.map(fileStatus =>
      new CsvReaderFactory(fileStatus.getPath.toString, schema)
    )
  }
}
