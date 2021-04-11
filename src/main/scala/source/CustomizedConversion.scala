package com.justinhsz
package source

import org.apache.hadoop.fs.RemoteIterator

import scala.language.implicitConversions

class RemoteIteratorWrapper[T](remoteIterator: RemoteIterator[T]) extends Iterator[T] {
  override def hasNext: Boolean = remoteIterator.hasNext
  override def next(): T = remoteIterator.next()
}

object CustomizedConversion {
  implicit def remoteIteratorToIterator[T](remoteIterator: RemoteIterator[T]): RemoteIteratorWrapper[T] =
    new RemoteIteratorWrapper(remoteIterator)
}
