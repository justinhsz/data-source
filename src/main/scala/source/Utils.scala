package com.justinhsz
package source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object Utils {
  lazy val fs: FileSystem = FileSystem.get(new Configuration())
}
