/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers.jdbc

import java.io.FileReader
import java.sql.{Connection, ResultSet}

import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.collection.mutable
import scala.io.Source

case class Table (name: String,
                  jdbcConnection: Connection) {

  /**
    * Builds and executes SQL statement and returns the ResultSet
    *
    * @param aggregations Sequence of aggregation functions
    * @return Returns ResultSet of the query
    */
  def executeAggregations(aggregations: Seq[String]): JdbcRow = {

      val query =
        s"""
           |SELECT
           | ${aggregations.mkString(", ")}
           |FROM
           | $name
        """.stripMargin

      val result = jdbcConnection.createStatement().executeQuery(query)
      // TODO: Test return value of next() and throw exception
      result.next()
      JdbcRow.from(result)
  }


  private[jdbc] def columns(): mutable.LinkedHashMap[String, String] = {

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | $name
         |LIMIT 0
        """.stripMargin

    val result = jdbcConnection.createStatement().executeQuery(query)

    // TODO: Test return value of next() and throw exception
    val metaData = result.getMetaData
    val colCount = metaData.getColumnCount

    var cols = mutable.LinkedHashMap[String, String]()
    for (col <- 1 to colCount) {
      cols(metaData.getColumnLabel(col)) = metaData.getColumnType(col) match {
        case 0 => "TEXT" // TODO: temporary fix for issues regarding SQLite meta data queries
        case _ => metaData.getColumnTypeName(col)
      }
    }

    cols
  }
}


object Table {

  def fromCsv(table: Table,
              csvFilePath: String,
              delimiter: String = ","): Table = {

    val src = Source.fromFile(csvFilePath)
    var cols = mutable.LinkedHashMap[String, String]()

    src.getLines().next().split(delimiter).foreach(colName => cols += (colName -> "TEXT"))

    create(table, cols)
    fillWithCsv(table, csvFilePath, delimiter)
  }

  def create(table: Table,
             columns: mutable.LinkedHashMap[String, String]): Table = {

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | ${table.name}
      """.stripMargin

    val stmt = table.jdbcConnection.createStatement()
    stmt.execute(deletionQuery)

    val creationQuery =
      s"""
         |CREATE TABLE
         | ${table.name}
         |  ${columns map { case (key, value) => s"$key $value" } mkString("(", ",", ")")}
     """.stripMargin

    stmt.execute(creationQuery)

    table
  }

  private def fill(table: Table,
           columns: mutable.LinkedHashMap[String, String],
           frequencies: Map[Seq[String], Long]): Table = {

    if (frequencies.nonEmpty) {

      val values = frequencies.map(entry =>
        s"(${entry._1.mkString("'", "', '", "'")}, '${entry._2}')").mkString(", ")

      val query =
        s"""
           |INSERT INTO
           | ${table.name} ${columns.keys.mkString("(", ", ", ")")}
           |VALUES
           | $values
       """.stripMargin

      val stmt = table.jdbcConnection.createStatement()
      stmt.execute(query)
    }

    table
  }

  private def fillWithCsv(table: Table, csvFilePath: String, delimiter: String = ",") : Table = {
    val copMan = new CopyManager(table.jdbcConnection.asInstanceOf[BaseConnection])
    val fileReader = new FileReader(csvFilePath)
    copMan.copyIn(s"COPY ${table.name} FROM STDIN DELIMITER '$delimiter' CSV HEADER", fileReader)
    table
  }

  def createAndFill(table: Table,
                    columns: mutable.LinkedHashMap[String, String],
                    frequencies: Map[Seq[String], Long]): Table = {

    create(table, columns)
    fill(table, columns, frequencies)
  }
}


case class JdbcRow(row: Seq[Any]) {

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

  def getAs[T](col: Int): T = row(col).asInstanceOf[T]
}

object JdbcRow {

  def from(result: ResultSet): JdbcRow = {
    var row = Seq.empty[Any]
    val numColumns = result.getMetaData.getColumnCount

    for (col <- 1 to numColumns) {
      row = row :+ result.getObject(col)
    }

    JdbcRow(row)
  }
}
