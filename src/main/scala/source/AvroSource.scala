package com.justinhsz.source

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{AvroFSInput, FileContext, FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConversions._

class PartitionedFileReader(filePath: String) extends DataReader[Row] {
  lazy val reader = {
    val fc = FileContext.getFileContext()
    val fileType = filePath.split('.').last.toLowerCase
    fileType match {
      case "avro" => new DataFileReader(new AvroFSInput(fc, new Path(filePath)), new GenericDatumReader[GenericRecord]())
      case _ => throw new NotImplementedError(s"The file type is not supported. File name: $filePath")
    }
  }

  lazy val fields = reader.getSchema.getFields
  lazy val fieldNum = fields.length

  override def next(): Boolean = reader.hasNext

  override def get(): Row = {
    val record = reader.next()
    Row.fromSeq((0 to fieldNum - 1).map{ idx =>
      val value = record.get(idx)
      if (value == null) null else value.toString
    })
  }

  override def close(): Unit = reader.close()
}

class PartitionedFileReaderFactory(filePath: String) extends DataReaderFactory[Row] {
  override def createDataReader(): PartitionedFileReader = new PartitionedFileReader(filePath)
}

class PartitionedFileSource extends ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = new DataSourceReader {
    val path = options.get("path").orElse("")
    val fileExtension = options.get("fileExtension").orElse("")

    val fileList = {
      lazy val fs = FileSystem.get(new Configuration())
      val list = new util.ArrayList[String]()
      val flattenList = fs.listFiles(new Path(path), true)
      while(flattenList.hasNext) {
        val currentPath = flattenList.next().getPath.toString
        if(currentPath.toLowerCase.endsWith(fileExtension)) {
          list.add(currentPath)
        }
      }
      fs.close()
      list
    }

    def toSparkType(name: String, schema: Schema, nullable: Boolean = false): StructField = {
      schema.getType match {
        case Type.STRING  => StructField(name, StringType, nullable)
        case Type.INT     => StructField(name, IntegerType, nullable)
        case Type.FLOAT   => StructField(name, FloatType, nullable)
        case Type.DOUBLE  => StructField(name, DoubleType, nullable)
        case Type.LONG    => StructField(name, LongType, nullable)
        case Type.BOOLEAN => StructField(name, BooleanType, nullable)
        case Type.NULL    => StructField(name, NullType, true)
        case Type.UNION   =>
          val types = schema.getTypes
          val (nullType, otherTypes) = types.partition(_.getType == Type.NULL)
          if(otherTypes.length == 1) {
            toSparkType(name, otherTypes.head, nullType.nonEmpty)
          } else {
            StructField(name, StringType, nullable)
          }
        case anyType: Schema.Type =>
          throw new NotImplementedError(s"Not support avro data type: ${anyType.getName}")
      }
    }

    override def readSchema(): StructType = {
      val reader = new PartitionedFileReader(fileList.head)
      val schema = StructType(reader.fields.map{ field => toSparkType(field.name(), field.schema())})
      reader.close()
      schema
    }

    override def createDataReaderFactories() = {
      fileList.map(new PartitionedFileReaderFactory(_))
    }
  }
}