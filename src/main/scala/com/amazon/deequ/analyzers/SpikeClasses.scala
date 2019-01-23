package com.amazon.deequ.analyzers

import java.sql.ResultSet

import org.apache.spark.sql.Row

case class AggregationResult(row: Seq[Any]) {

  def getLong(col: Int): Long = {

    row(col) match {
      case number: Number => number.longValue()
      case null => 0
      case _ => throw new IllegalArgumentException("No numeric type")
    }
  }

  def getDouble(col: Int): Double = {

    row(col) match {
      case number: Number => number.doubleValue()
      case null => 0.0
      case _ => throw new IllegalArgumentException("No numeric type")
    }
  }

  def getObject(col: Int): Any = {
    row(col)
  }

  def isNullAt(col: Int): Boolean = {
    row(col) == null
  }
}

object AggregationResult {

  def from(result: Row): AggregationResult = {
    var row = Seq.empty[Any]
    val numColumns = result.size

    for (col <- 0 until numColumns) {
      row = row :+ result.get(col)
    }

    AggregationResult(row)
  }

  def from(result: ResultSet): AggregationResult = {
    var row = Seq.empty[Any]
    val numColumns = result.getMetaData.getColumnCount

    for (col <- 1 to numColumns) {
      row = row :+ result.getObject(col)
    }

    AggregationResult(row)
  }
}