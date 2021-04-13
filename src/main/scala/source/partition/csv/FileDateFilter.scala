package com.justinhsz
package source.partition.csv

import com.github.nscala_time.time.Imports.{LocalDate, Period}

import scala.collection.mutable.ArrayBuffer

class FileDateFilter(private var dateList: Array[LocalDate] = Array.empty[LocalDate]) {
  private var lowerBound: LocalDate = null
  private var lowerBoundInclude: Boolean = null
  private var upperBound: LocalDate = null
  private var upperBoundInclude: Boolean = null

  def this(date: LocalDate) = this(Array(date))

  def setLower(lowerBound: LocalDate, lowerBoundInclude: Boolean) = {
    if(this.lowerBound == null || lowerBound.isAfter(this.lowerBound)) {
      this.lowerBound = lowerBound
      this.lowerBoundInclude = lowerBoundInclude
    }
    this
  }

  def setUpper(upperBound: LocalDate, upperBoundInclude: Boolean) = {
    if(this.upperBound == null || upperBound.isBefore(this.upperBound)){
      this.upperBound = upperBound
      this.upperBoundInclude = upperBoundInclude
    }
    this
  }

  def getList = {
    if(dateList.isEmpty && lowerBound != null && upperBound != null && lowerBound.isBefore(upperBound)){
      val b = ArrayBuffer.empty[LocalDate]
      var currentDate = new LocalDate(lowerBound)
      if(!lowerBoundInclude) {
        currentDate = currentDate.plus(Period.days(1))
      }

      while(currentDate.isBefore(upperBound)) {
        b.append(currentDate)
        currentDate.plus(Period.days(1))
      }

      if(upperBoundInclude) {
        b.append(currentDate)
      }
      dateList = b.toArray
    }
    dateList
  }

  def +(dateFilter: FileDateFilter) = new FileDateFilter(this.getList ++ dateFilter.getList)

  def -(dateFilter: FileDateFilter) = new FileDateFilter(this.getList.intersect(dateFilter.getList))
}