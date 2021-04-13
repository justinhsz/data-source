package com.justinhsz
package source.partition.csv

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}

class CsvSource extends ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = new CsvDataSourceReader(options)
}

