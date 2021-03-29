package com.justinhsz.partitioned

import org.apache.avro.Schema.Type
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{AvroFSInput, FileContext, FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConversions._

class PartitionedFileReader(filePath: Path) extends DataReader[Row] {
  lazy val reader = {
    val fc = FileContext.getFileContext()
    val fileType = filePath.getName.split('.').last.toLowerCase
    fileType match {
      case "avro" => new DataFileReader(new AvroFSInput(fc, filePath), new GenericDatumReader[GenericRecord]())
      case _ => throw new NotImplementedError(s"The file type is not supported. File name: ${filePath.getName}")
    }
  }

  lazy val fields = reader.getSchema.getFields
  lazy val fieldNum = fields.length

  override def next(): Boolean = reader.hasNext

  override def get(): Row = {
    val record = reader.next()
    Row.fromSeq((0 to fieldNum).map(record.get))
  }

  override def close(): Unit = reader.close()
}

class PartitionedFileReaderFactory(filePath: Path) extends DataReaderFactory[Row] {
  override def createDataReader(): PartitionedFileReader = new PartitionedFileReader(filePath)
}
object PartitionedFileSource {
  val version = "1.0.0"
}

class PartitionedFileSource extends ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = new DataSourceReader {
    val path = options.get("path").orElse("")
    val fileExtension = options.get("fileExtension").orElse("")

    val fileList = {
      lazy val fs = FileSystem.get(new Configuration())
      val list = new util.ArrayList[Path]()
      val flattenList = fs.listFiles(new Path(path), true)
      while(flattenList.hasNext) {
        val currentPath = flattenList.next().getPath
        if(currentPath.getName.toLowerCase.endsWith(fileExtension)) {
          list += flattenList.next().getPath
          println(currentPath.getName)
        }
      }
      fs.close()
      list
    }

    override def readSchema(): StructType = {
      val reader = new PartitionedFileReader(fileList.head)
      val schema = StructType(reader.fields.map{ field =>
        val name = field.name()
        val dataType = field.schema().getType match {
          case Type.STRING => StringType
          case Type.INT => IntegerType
          case Type.FLOAT => FloatType
          case Type.DOUBLE => DoubleType
          case Type.LONG => LongType
          case Type.BOOLEAN => BooleanType
          case anyType: Type => throw new NotImplementedError(s"Not support avro data type: ${anyType.getName}")
        }
        StructField(name, dataType)
      })
      reader.close()
      schema
    }

    override def createDataReaderFactories() = {
      fileList.filter(_.getName.toLowerCase.endsWith(fileExtension)).map(new PartitionedFileReaderFactory(_))
    }
  }
}