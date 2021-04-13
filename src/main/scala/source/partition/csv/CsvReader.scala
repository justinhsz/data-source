package com.justinhsz
package source.partition.csv

import source.Utils.fs

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.StructType

import java.io.InputStreamReader
import java.util
import scala.util.Try

class CsvReader(filePath: String, schema: Option[StructType] = None) extends DataReader[Row] {
  private lazy val inputStream = fs.open(new Path(filePath))

  private lazy val reader = CSVFormat.DEFAULT
    .withFirstRecordAsHeader()
    .parse(new InputStreamReader(inputStream))

  lazy val fileFields: util.List[String] = reader.getHeaderNames

  private lazy val records = reader.iterator()

  override def next(): Boolean = records.hasNext

  override def get(): Row = {
    require(schema.isDefined, "CsvReader not support reading rows without giving schema.")

    val record = records.next()
    val values = schema.get.fieldNames.map { field =>
      Try(record.get(field)).getOrElse(null)
    }

    Row.fromSeq(values)
  }

  override def close(): Unit = inputStream.close()
}
