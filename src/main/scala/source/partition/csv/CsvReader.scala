package com.justinhsz
package source.partition.csv

import source.Utils.fs

import com.github.nscala_time.time.Imports._
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.StructType

import java.io.InputStreamReader
import java.util
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

class CsvReader(filePath: String, schema: Option[StructType] = None) extends DataReader[Row] {
  private val path = new Path(filePath)
  private lazy val fileDate: java.sql.Date = {
    @tailrec
    def extractAttrFromPath(path: Path,
                            param: Map[String, String] = Map.empty[String, String]): Map[String, String] = {
      path.getName.split("=") match {
        case Array(key, value) => extractAttrFromPath(path.getParent, param + ((key, value)))
        case _ => param
      }
    }

    val attrs = extractAttrFromPath(path.getParent)

    Try(new java.sql.Date(
      new LocalDate()
        .withYear(attrs("year").toInt)
        .withMonthOfYear(attrs("month").toInt)
        .withDayOfMonth(attrs("day").toInt).toDate.getTime
    )).getOrElse(null)
  }
  private lazy val inputStream = fs.open(path)

  private lazy val reader = CSVFormat.DEFAULT
    .withFirstRecordAsHeader()
    .parse(new InputStreamReader(inputStream))

  lazy val fileFields: util.List[String] = reader.getHeaderNames

  private lazy val records = reader.iterator()

  override def next(): Boolean = records.hasNext

  override def get(): Row = {
    require(schema.isDefined, "CsvReader not support reading rows without giving schema.")
    val record = records.next()
    val values: Array[Any] = schema.get.fieldNames.map { field =>
      Try(record.get(field)).getOrElse{
        if(field == "file_date") fileDate else null
      }
    }
    Row.fromSeq(values)
  }

  override def close(): Unit = inputStream.close()
}
