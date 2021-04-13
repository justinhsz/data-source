package com.justinhsz
package source.partition.csv

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType

class CsvReaderFactory(path: String, schema: StructType) extends DataReaderFactory[Row] {
  override def createDataReader(): CsvReader = new CsvReader(path, Some(schema))
}
