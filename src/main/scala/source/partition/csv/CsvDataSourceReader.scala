package com.justinhsz
package source.partition.csv

import source.CustomizedConversion._
import source.Utils.fs

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util
import scala.collection.JavaConversions._
import scala.util.Try

class CsvDataSourceReader(options: DataSourceOptions) extends DataSourceReader with SupportsPushDownFilters {
  private val rawPath = options.get("path")
  require(rawPath.isPresent, "require giving the path.")

  private val path = rawPath.get()

  private var fileList = Array.empty[LocatedFileStatus]

  override def readSchema(): StructType = {
    val reader = new CsvReader(fileList.head.getPath.toString)
    val schema = StructType(reader.fileFields.map { field => StructField(field, StringType) })
    reader.close()
    schema
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val schema = readSchema()
    fileList.toList.map(fileStatus =>
      new CsvReaderFactory(fileStatus.getPath.toString, schema)
    )
  }

  def asDate(value: Any): LocalDate = {
    case date: java.sql.Date           => LocalDate.fromDateFields(date)
    case timestamp: java.sql.Timestamp => timestamp.toLocalDate
    case long: Long                    => new LocalDateTime(long).toLocalDate
    case dateString: String            =>
      Try(LocalDate.parse(dateString))
        .orElse(Try(LocalDateTime.parse(dateString)))
        .get
    case _ => throw new UnsupportedOperationException(s"The input date type is not able to transform as DateTYpe, value class name: ${value.getClass.getName}, value: ${value.toString}")
  }

  def fileDateFilter(filter: Filter, dateFilter: FileDateFilter = new FileDateFilter()): FileDateFilter = {
    filter match {
      case EqualTo("file_date", value)            => new FileDateFilter(asDate(value)) + dateFilter
      case In("file_date", values)                => new FileDateFilter(values.map{value => asDate(value)}) + dateFilter

      case GreaterThan("file_date", value)        => dateFilter.setLower(asDate(value), false)
      case GreaterThanOrEqual("file_date", value) => dateFilter.setLower(asDate(value), true)
      case LessThan("file_date", value)           => dateFilter.setUpper(asDate(value), false)
      case LessThanOrEqual("file_date", value)    => dateFilter.setUpper(asDate(value), true)

      case And(left, right)                       => fileDateFilter(right, fileDateFilter(left, dateFilter)) + dateFilter
      case _ =>
        println("Not support OR filter to filter file date.")
        dateFilter
    }
  }

  def updatePaths(fileDates: Array[LocalDate]) = {
    fileList = fileDates.flatMap{ fileDate =>
      val filePath = new Path(s"$path/year=${fileDate.getYear}/month=${fileDate.getMonthOfYear}")
      if(fs.exists(filePath) && fs.isDirectory(filePath)) {
        fs.listFiles(filePath, true).filter(_.getPath.getName.toLowerCase.endsWith(".csv"))
      } else {
        Array.empty
      }
    }
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val fileDates = filters.map(fileDateFilter(_)).reduce(_ + _).getList
    updatePaths(fileDates)
    filters
  }

  override var pushedFilters: Array[Filter] = Array.empty
}
