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

import java.util.UUID

import com.amazon.deequ.analyzers.Analyzers
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.analyzers.jdbc.Preconditions._
import com.amazon.deequ.metrics.DoubleMetric

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class JdbcFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends JdbcGroupingAnalyzer[JdbcFrequenciesAndNumRows, DoubleMetric] {

  def toDouble(input: String): String = {
    s"CAST($input AS DOUBLE PRECISION)"
  }

  def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(table: Table): Option[JdbcFrequenciesAndNumRows] = {
    Some(JdbcFrequencyBasedAnalyzer.computeFrequencies(table, groupingColumns()))
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  protected def toSuccessMetric(value: Double): DoubleMetric = {
    metricFromValue(value, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  protected def emptyFailureMetric(): DoubleMetric = {
    metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[Table => Unit] = {
    super.preconditions ++ Seq(atLeastOne(columnsToGroupOn)) ++
      columnsToGroupOn.map { hasColumn }
  }
}


object JdbcFrequencyBasedAnalyzer {

  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequencies(
    table: Table,
    groupingColumns: Seq[String],
    numRows: Option[Long] = None)
  : JdbcFrequenciesAndNumRows = {

    val connection = table.jdbcConnection
    val frequenciesTable = Table(JdbcFrequencyBasedAnalyzerUtils.uniqueTableName(), connection)

    val oneOfGroupingColumnsNull = groupingColumns.map(col => col + " IS NULL").mkString(" OR ")
    def caseOneColumnIsNull(col: String) = {
      s"(CASE WHEN $oneOfGroupingColumnsNull THEN NULL ELSE $col END)"
    }

    val selectColumns = groupingColumns.map(col =>
      s"${caseOneColumnIsNull(col)} AS $col").mkString(", ")
    val groupByColumns = groupingColumns.map(col => caseOneColumnIsNull(col)).mkString(", ")

    val query =
      s"""
         |CREATE TEMPORARY TABLE ${frequenciesTable.name} AS
         | SELECT $selectColumns, COUNT(*) AS ${Analyzers.COUNT_COL}
         |    FROM ${table.name}
         |    GROUP BY $groupByColumns
        """.stripMargin

    val statement = connection.createStatement()
    statement.execute(query)

    JdbcFrequenciesAndNumRows(frequenciesTable, numRows)
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class JdbcScanShareableFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends JdbcFrequencyBasedAnalyzer(name, columnsToGroupOn) {

  def aggregationFunctions(numRows: Long): Seq[String]

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>

        val aggregations = aggregationFunctions(theState.numRows())

        val result = theState.table.executeAggregations(aggregations)

        fromAggregationResult(result, 0)
      case None =>
        metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    }
  }

  def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    if (result.isNullAt(offset)) {
      emptyFailureMetric()
    } else {
      toSuccessMetric(result.getDouble(offset))
    }
  }
}

object JdbcFrequencyBasedAnalyzerUtils {

  def uniqueTableName(): String = {
    s"__deequ__frequenciesAndNumRows_${
      UUID.randomUUID().toString
        .replace("-", "")
    }"
  }

  private[jdbc] def join(first: Table,
                         second: Table): Table = {

    val table = Table(uniqueTableName(), first.jdbcConnection)
    val columns = first.columns()
    val groupingColumns = columns.keys.toSeq.filter(col => col != "absolute")

    if (columns == second.columns()) {

      val query =
        s"""
           |CREATE TEMPORARY TABLE
           | ${table.name}
           |AS
           | SELECT
           |  ${groupingColumns.mkString(", ")},
           |  SUM(${Analyzers.COUNT_COL}) as ${Analyzers.COUNT_COL}
           | FROM (
           |  SELECT * FROM ${first.name}
           |   UNION ALL
           |  SELECT * FROM ${second.name}) AS combinedState
           | GROUP BY
           |  ${groupingColumns.mkString(", ")}
        """.stripMargin

      table.jdbcConnection.createStatement().execute(query)
      table
    }
    else {
      throw new IllegalArgumentException("Cannot join tables with different columns")
    }
  }
}

