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
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

import java.util
import scala.collection.JavaConversions._
import scala.util.Try

class CsvDataSourceReader(options: DataSourceOptions) extends DataSourceReader with SupportsPushDownFilters {
  private val rawPath = options.get("path")
  require(rawPath.isPresent, "require giving the path.")

  private val path = rawPath.get()

  private var fileList = Array.empty[LocatedFileStatus]

  override def readSchema(): StructType = {
    val reader = new CsvReader(getFirstFile.getPath.toString)
    val fields = reader.fileFields.map { field =>
      StructField(field, StringType)
    }
    fields.append(StructField("file_date", DateType))
    val schema = StructType(fields)
    reader.close()
    schema
  }

  def getFirstFile() = {
    fs.listStatus(new Path(path)).toIterator.flatMap { fileOrDir =>
      if (fileOrDir.isDirectory) {
        fs.listFiles(fileOrDir.getPath, true)
      } else {
        fs.listLocatedStatus(fileOrDir.getPath)
      }
    }.filter(_.getPath.getName.toLowerCase.endsWith(".csv")).next()
  }

  def updateFileList() = {
    fileList = fs.listStatus(new Path(path)).flatMap { fileOrDir =>
      if (fileOrDir.isDirectory) {
        fs.listFiles(fileOrDir.getPath, true)
      } else {
        fs.listLocatedStatus(fileOrDir.getPath)
      }
    }
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    if(fileList.isEmpty) {
      println("empty file list, now loading all files.")
      updateFileList()
    }
    val schema = readSchema()
    fileList.filter(_.getPath.getName.toLowerCase.endsWith(".csv")).map(fileStatus =>
      new CsvReaderFactory(fileStatus.getPath.toString, schema)
    ).toList
  }

  def asDate(value: Any): LocalDate = value match {
    case date: java.sql.Date           => LocalDate.fromDateFields(date)
    case timestamp: java.sql.Timestamp => timestamp.toLocalDate
    case long: Long                    => new LocalDateTime(long).toLocalDate
    case dateString: String            =>
      Try(LocalDate.parse(dateString))
        .orElse(Try(LocalDateTime.parse(dateString).toLocalDate))
        .get
    case _ => throw new UnsupportedOperationException(s"The input date type is not able to transform as DateTYpe, value class name: ${value.getClass.getName}, value: ${value.toString}")
  }

  def fileDateFilter(filter: Filter, dateFilter: FileDateFilter = new FileDateFilter()): FileDateFilter = {
    filter match {
      case EqualTo("file_date", value) => new FileDateFilter(asDate(value)) + dateFilter
      case In("file_date", values) => new FileDateFilter(values.map{value => asDate(value)}) + dateFilter

      case GreaterThan("file_date", value) => dateFilter.setLower(asDate(value), false)
      case GreaterThanOrEqual("file_date", value) => dateFilter.setLower(asDate(value), true)
      case LessThan("file_date", value) => dateFilter.setUpper(asDate(value), false)
      case LessThanOrEqual("file_date", value) => dateFilter.setUpper(asDate(value), true)

      case And(left, right) => fileDateFilter(right, fileDateFilter(left, dateFilter)) + dateFilter
      case IsNotNull(attribute) => new FileDateFilter()
      case otherFilter: Filter =>
        println(s"Not support ${otherFilter.toString} to filter file date.")
        dateFilter
    }
  }

  def updatePaths(fileDates: Array[LocalDate]) = {
    fileList = fileDates.flatMap{ fileDate =>
      val filePath = new Path(s"$path/year=${fileDate.getYear}/month=${fileDate.getMonthOfYear}/day=${fileDate.getDayOfMonth}")
      if(fs.exists(filePath) && fs.isDirectory(filePath)) {
        fs.listFiles(filePath, true).filter(_.getPath.getName.toLowerCase.endsWith(".csv"))
      } else {
        Array.empty[LocatedFileStatus]
      }
    }.distinct
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    println("now push down date filter...")
    val fileDates = filters.map(fileDateFilter(_)).reduce(_ + _).getList
    updatePaths(fileDates)
    filters
  }

  override val pushedFilters = Array.empty[Filter]
}
