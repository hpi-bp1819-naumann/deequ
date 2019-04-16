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
import java.util.UUID

import com.amazon.deequ.runtime.jdbc.operators.FrequencyBasedOperatorsUtils.uniqueTableName
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.io.Source

case class Table (name: String,
                  jdbcConnection: Connection,
                  temporary: Boolean = false) {

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

    val stmt = jdbcConnection.createStatement()
    val result = stmt.executeQuery(query)
    // TODO: Test return value of next() and throw exception
    result.next()
    val aggResult = JdbcRow.from(result)

    try {
      aggResult
    } finally {
      result.close()
      stmt.close()
    }
  }

  private[this] def existsWorkaround(): Boolean = {

    val query =
      s"""
         |SELECT
         |  name
         |FROM
         |  (SELECT * FROM sqlite_master
         |  UNION ALL
         |  SELECT * FROM sqlite_temp_master)
         |WHERE
         |  type='table'
         |  AND name='$name'
     """.stripMargin

    val stmt = jdbcConnection.createStatement()
    val result = stmt.executeQuery(query)

    try {
      result.next()
    } finally {
      stmt.close()
    }
  }

  def exists(): Boolean = {

    // Workaround for SQLite
    if (isSQLite()) {
      existsWorkaround()
    } else {

      val metaData = jdbcConnection.getMetaData
      val result = metaData.getTables(null, null, null, null) //Array[String]("TABLE"))

      var hasTable = false

      while (result.next()) {
        if (result.getString("TABLE_NAME") == name.toLowerCase) {
          hasTable = true
        }
      }

      hasTable
    }
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


  /*private[deequ] def columns(): mutable.LinkedHashMap[String, JdbcDataType] = {

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | $name
         |LIMIT 0
        """.stripMargin

    val stmt = jdbcConnection.createStatement()
    val result = stmt.executeQuery(query)

    // TODO: Test return value of next() and throw exception
    val metaData = result.getMetaData
    val colCount = metaData.getColumnCount

    var cols = mutable.LinkedHashMap[String, JdbcDataType]()
    for (col <- 1 to colCount) {
      cols(metaData.getColumnLabel(col)) = JdbcDataType.fromSqlType(metaData.getColumnType(col))
    }

    result.close()
    stmt.close()

    cols
  }*/

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

  def union(other: Table, beTemporary: Boolean = false): Table = {

    val table = Table(uniqueTableName(), jdbcConnection)

    if (schema() == other.schema()) {

      val temporaryOption = beTemporary match {
        case true => " TEMPORARY"
        case false => ""
      }

      val query =
        s"""
           |CREATE$temporaryOption TABLE
           | ${table.name}
           |AS
           | SELECT *
           | FROM (
           |  SELECT * FROM $name
           |   UNION ALL
           |  SELECT * FROM ${other.name})
           | AS newTable
        """.stripMargin

      table.execute(query)
      table
    }
    else {
      throw new IllegalArgumentException("Cannot join tables with different schemas")
    }
  }

  private[deequ] def withColumn(columnName: String, dataType: JdbcDataType, fillQuery: Option[String]): Table = {

    var query = s"ALTER TABLE $name ADD COLUMN $columnName ${dataType.toString()}"
    val stmt = jdbcConnection.createStatement()
    stmt.execute(query)
    stmt.close()

    if (fillQuery.isDefined) {
      query = s"UPDATE $name SET $columnName = ${fillQuery.get}"
      val stmt = jdbcConnection.createStatement()
      stmt.execute(query)
      stmt.close()
    }

    this
  }

  private[deequ] def execute(sql: String): Unit = {
    val stmt = jdbcConnection.createStatement()
    stmt.execute(sql)
    stmt.close()
  }

  private[deequ] def isSQLite(): Boolean = {
    jdbcConnection.getMetaData.getDatabaseProductName == "SQLite"
  }

  private[this] def dropWorkaround(columnName: String): Unit = {

    val cols = (schema().columnsNamesAsSet() - columnName).toSeq.mkString(", ")
    val tempTableName = s"${name}_${UUID.randomUUID().toString.replace("-", "")}"

    var query =
      s"""
         |CREATE TABLE $tempTableName AS
         | SELECT $cols
         | FROM $name;
        """.stripMargin
    execute(query)

    query =
      s"""
         |DROP TABLE $name;
        """.stripMargin
    execute(query)

    query =
      s"""
         |ALTER TABLE $tempTableName RENAME TO $name;
        """.stripMargin
    execute(query)
  }

  private[deequ] def drop(columnName: String): Table = {

    // dropping columns is not supported in SQLite
    if (isSQLite()) {
      dropWorkaround(columnName)
    } else {
      val query = s"ALTER TABLE $name DROP COLUMN $columnName"
      execute(query)
    }

    this
  }

  private[this] def withColumnRenamedWorkaround(oldColumn: String, newColumn: String): Unit = {

    val cols = (schema().columnsNamesAsSet() - oldColumn).toSeq
    val columnsWithRenamed = (cols :+ s"$oldColumn AS $newColumn").mkString(", ")

    val tempTableName = s"${name}_${UUID.randomUUID().toString.replace("-", "")}"

    val query =
      s"""
         |CREATE TABLE $tempTableName AS
         | SELECT $columnsWithRenamed
         | FROM $name;
        """.stripMargin
    execute(query)
    execute(s"DROP TABLE $name;")
    execute(s"ALTER TABLE $tempTableName RENAME TO $name")
  }

  private[deequ] def withColumnRenamed(oldColumn: String, newColumn: String): Table = {

    // renaming columns is not supported in SQLite
    if (isSQLite()) {
      withColumnRenamedWorkaround(oldColumn, newColumn)
    } else {
      val query = s"ALTER TABLE $name RENAME COLUMN $oldColumn TO $newColumn"
      execute(query)
    }

    this
  }
}


object Table {

  def fromCsv(table: Table,
              csvFilePath: String,
              delimiter: String = ","): Table = {

    val src = Source.fromFile(csvFilePath)
    var cols = Seq.empty[JdbcStructField]

    src.getLines().next().split(delimiter).foreach(colName => cols = cols :+ JdbcStructField(colName, StringType))

    val schema = JdbcStructType(cols)
    create(table, schema)
    fillWithCsv(table, csvFilePath, delimiter)
  }

  def create(table: Table,
             schema: JdbcStructType): Table = {

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | ${table.name}
      """.stripMargin

    val stmt = table.execute(deletionQuery)

    val temporary = table.temporary match {
      case true => " TEMPORARY"
      case false => ""
    }

    val creationQuery =
      s"""
         |CREATE$temporary TABLE
         | ${table.name}
         |  ${schema.toString}
     """.stripMargin

    table.execute(creationQuery)

    table
  }

  private def fill(table: Table,
                   schema: JdbcStructType,
                   frequencies: Map[Seq[String], Long]): Table = {

    if (frequencies.nonEmpty) {

      val values = frequencies.map(entry =>
        s"(${entry._1.mkString("'", "', '", "'")}, '${entry._2}')").mkString(", ")

      val query =
        s"""
           |INSERT INTO
           | ${table.name} ${schema.toStringWithoutDataTypes}
           |VALUES
           | $values
       """.stripMargin

      table.execute(query)
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
                    schema: JdbcStructType,
                    frequencies: Map[Seq[String], Long]): Table = {

    create(table, schema)
    fill(table, schema, frequencies)
  }
}


case class JdbcRow(var row: Seq[Any]) {

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

  def setObject(col: Int, that: Any): Unit = {
    row = row.updated(col, that)
  }
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