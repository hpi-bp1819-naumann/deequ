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

import java.sql.{Connection, ResultSet}

import com.amazon.deequ.analyzers.{Analyzers, State}

import scala.collection.mutable

case class JdbcFrequenciesAndNumRows(table: Table,
                                     private var _numRows: Option[Long] = None,
                                     private var _numNulls: Option[Long] = None)
  extends State[JdbcFrequenciesAndNumRows] {


  def numNulls(): Long = {
    _numNulls match {
      case None =>
        val firstGroupingColumn = table.columns().head._1

        val numNulls =
          s"SUM(CASE WHEN ($firstGroupingColumn IS NULL) THEN ${Analyzers.COUNT_COL} ELSE 0 END)"

        val result = table.executeAggregations(numNulls :: Nil)

        _numNulls = Some(result.getLong(0))
      case Some(_) =>
    }

    _numNulls.get
  }

  def numRows(): Long = {
    _numRows match {
      case None =>
        val numRows = s"SUM(${Analyzers.COUNT_COL})"

        val result = table.executeAggregations(numRows :: Nil)

        _numRows = Some(result.getLong(0))
      case Some(_) =>
    }

    _numRows.get
  }

  def columns(): mutable.LinkedHashMap[String, String] = {
    table.columns()
  }

  def frequencies(): Map[Seq[String], Long] = {

    var frequencies = Map[Seq[String], Long]()

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | ${table.name}
  """.stripMargin

    val statement = table.jdbcConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()
    val numGroupingColumns = result.getMetaData.getColumnCount - 1

    while (result.next()) {
      val columns = (1 to numGroupingColumns).map(col => result.getString(col)).seq
      frequencies += (columns -> result.getLong(numGroupingColumns + 1))
    }

    frequencies
  }

  def columnsAndFrequencies(): (mutable.LinkedHashMap[String, String], Map[Seq[String], Long]) = {
    (columns(), frequencies())
  }

  override def sum(other: JdbcFrequenciesAndNumRows): JdbcFrequenciesAndNumRows = {

    val totalRows = numRows + other.numRows
    val newTable = JdbcFrequencyBasedAnalyzerUtils.join(table, other.table)

    JdbcFrequenciesAndNumRows(newTable, Some(totalRows), Some(numNulls() + other.numNulls()))
  }
}

object JdbcFrequenciesAndNumRows {

  def from(connection: Connection,
           columns: mutable.LinkedHashMap[String, String],
           frequencies: Map[Seq[String], Long], numRows: Long): JdbcFrequenciesAndNumRows = {

    val table = Table(JdbcFrequencyBasedAnalyzerUtils.uniqueTableName(), connection)
    Table.createAndFill(table, columns, frequencies)

    JdbcFrequenciesAndNumRows(table, Some(numRows))
  }
}
