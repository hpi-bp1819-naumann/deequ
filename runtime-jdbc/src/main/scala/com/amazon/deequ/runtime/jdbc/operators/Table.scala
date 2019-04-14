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

package com.amazon.deequ.runtime.jdbc.operators

import java.io.FileReader
import java.sql.{Connection, ResultSet}

import com.amazon.deequ.runtime.jdbc.operators.FrequencyBasedOperatorsUtils.uniqueTableName
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.collection.mutable
import scala.io.Source

case class Table (name: String,
                  jdbcConnection: Connection) {

  /**
    * Builds and executes SQL statement and returns the ResultSet
    *
    * @param agg Aggregation function
    * @param aggs Aggregation functions
    * @return Returns ResultSet of the query
    */
  def agg(agg: String, aggs: String*): JdbcRow = {

    val query =
      s"""
         |SELECT
         | ${(agg +: aggs).mkString(", ")}
         |FROM
         | $name
      """.stripMargin

    println(query)

    val result = jdbcConnection.createStatement().executeQuery(query)
    // TODO: Test return value of next() and throw exception
    result.next()
    JdbcRow.from(result)
  }

  private[deequ] def rows(): Seq[JdbcRow] = {
    val query =
      s"""
         |SELECT
         | *
         |FROM
         | $name
        """.stripMargin

    val result = jdbcConnection.createStatement().executeQuery(query)

    var rows = Seq.empty[JdbcRow]
    while (result.next()) {
      rows = rows :+ JdbcRow.from(result)
    }

    rows
  }


  private[deequ] def columns(): mutable.LinkedHashMap[String, JdbcDataType] = {

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

    var cols = mutable.LinkedHashMap[String, JdbcDataType]()
    for (col <- 1 to colCount) {
      cols(metaData.getColumnLabel(col)) = JdbcDataType.fromSqlType(metaData.getColumnType(col))
    }

    cols
  }

  private[deequ] def schema(): JdbcStructType = {

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

    var fields = Seq.empty[JdbcStructField]
    for (col <- 1 to colCount) {
      val name = metaData.getColumnLabel(col)
      val dataType = JdbcDataType.fromSqlType(metaData.getColumnType(col))

      fields = fields :+ JdbcStructField(name, dataType)
    }

    JdbcStructType(fields)
  }

  def union(other: Table): Table = {

    val table = Table(uniqueTableName(), jdbcConnection)

    if (columns() == other.columns()) {

      val query =
        s"""
           |CREATE TEMPORARY TABLE
           | ${table.name}
           |AS
           | SELECT *
           | FROM (
           |  SELECT * FROM $name
           |   UNION ALL
           |  SELECT * FROM ${other.name})
           | AS newTable
        """.stripMargin

      table.jdbcConnection.createStatement().execute(query)
      table
    }
    else {
      throw new IllegalArgumentException("Cannot join tables with different schemas")
    }
  }

  private[deequ] def withColumn(columnName: String, dataType: JdbcDataType, fillQuery: Option[String]): Table = {

    var query = s"ALTER TABLE $name ADD COLUMN $columnName ${dataType.toString()}"
    jdbcConnection.createStatement().execute(query)

    if (fillQuery.isDefined) {
      query = s"UPDATE $name SET $columnName = (SELECT ${fillQuery.get} FROM $name)"
      jdbcConnection.createStatement().execute(query)
    }

    this
  }

  private[deequ] def drop(columnName: String): Table = {

    val query = s"ALTER TABLE $name DROP $columnName"
    val stmt = jdbcConnection.createStatement()
    stmt.execute(query)

    this
  }

  private[deequ] def withColumnRenamed(oldColumn: String, newColumn: String): Table = {

    val query = s"ALTER TABLE $name RENAME $oldColumn TO $newColumn"
    val stmt = jdbcConnection.createStatement()
    stmt.execute(query)

    this
  }
}


object Table {

  def fromCsv(table: Table,
              csvFilePath: String,
              delimiter: String = ","): Table = {

    val src = Source.fromFile(csvFilePath)
    var cols = mutable.LinkedHashMap[String, JdbcDataType]()

    src.getLines().next().split(delimiter).foreach(colName => cols += (colName -> StringType))

    create(table, cols)
    fillWithCsv(table, csvFilePath, delimiter)
  }

  def create(table: Table,
             columns: mutable.LinkedHashMap[String, JdbcDataType]): Table = {

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
           columns: mutable.LinkedHashMap[String, JdbcDataType],
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
                    columns: mutable.LinkedHashMap[String, JdbcDataType],
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

object JdbcColumn {

  /*val StringType = StringType
  val IntegerType = "INT"
  val DoubleType = "DOUBLE"
  val DecimalType = "DECIMAL"
  val FloatType = "REAL"
  val TimestampType = "TIMESTAMP"
  val BooleanType = "BOOLEAN"
  val LongType = "BIGINT"
  val ShortType = "SMALLINT"
  val ByteType = "BYTEA"

  val numericTypes = Seq(DoubleType, DecimalType, FloatType, BooleanType, LongType, ShortType, ByteType)

  def isNumeric(dataType: String): Boolean = {
    numericTypes.exists(_.startsWith(dataType))
  }

  def DecimalType(p: Int, s: Int): String = {
    s"NUMERIC($p, $s)"
  }*/
}