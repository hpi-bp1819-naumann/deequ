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

import java.sql.{Connection, ResultSet}
import java.util.UUID

import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.runtime.jdbc.operators.Operators._
import com.amazon.deequ.runtime.jdbc.operators.Preconditions._

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class FrequencyBasedOperator(columnsToGroupOn: Seq[String])
  extends GroupingOperator[FrequenciesAndNumRows, DoubleMetric] {

  override def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(data: Table): Option[FrequenciesAndNumRows] = {
    Some(FrequencyBasedOperator.computeFrequencies(data, groupingColumns()))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[Table => Unit] = {
    super.preconditions ++ columnsToGroupOn.map { hasColumn } ++ Seq(atLeastOne(columnsToGroupOn))
  }
}

object FrequencyBasedOperator {

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
  : FrequenciesAndNumRows = {

    val connection = table.jdbcConnection
    val frequenciesTable = Table(FrequencyBasedOperatorsUtils.uniqueTableName(), connection)

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
         | SELECT $selectColumns, COUNT(*) AS ${Operators.COUNT_COL}
         |    FROM ${table.name}
         |    GROUP BY $groupByColumns
        """.stripMargin

    table.execute(query)

    FrequenciesAndNumRows(frequenciesTable, numRows)
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class ScanShareableFrequencyBasedOperator(name: String, columnsToGroupOn: Seq[String])
  extends FrequencyBasedOperator(columnsToGroupOn) {

  def aggregationFunctions(numRows: Long): Seq[String]

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        val aggregations = aggregationFunctions(theState.numRows)

        val result = theState.table.agg(aggregations.head, aggregations.tail: _*)

        fromAggregationResult(result, 0)

      case None =>
        metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    }
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

  def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    if (result.isNullAt(offset)) {
      metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    } else {
      toSuccessMetric(result.getDouble(offset))
    }
  }

}

/** State representing frequencies of groups in the data, as well as overall #rows */
case class FrequenciesAndNumRows(table: Table,
                                 private var _numRows: Option[Long] = None,
                                 private var _numNulls: Option[Long] = None)
  extends State[FrequenciesAndNumRows] {

  def numNulls(): Long = {
    _numNulls match {
      case None =>
        val firstGroupingColumn = table.schema().fields.head.name

        val numNulls =
          s"SUM(CASE WHEN ($firstGroupingColumn IS NULL) THEN ${Operators.COUNT_COL} ELSE 0 END)"

        val result = table.agg(numNulls)

        _numNulls = Some(result.getLong(0))
      case Some(_) =>
    }

    _numNulls.get
  }

  def numRows(): Long = {
    _numRows match {
      case None =>
        val numRows = s"SUM(${Operators.COUNT_COL})"

        val result = table.agg(numRows)

        _numRows = Some(result.getLong(0))
      case Some(_) =>
    }

    _numRows.get
  }

  /** Add up frequencies via an outer-join */
  override def sum(other: FrequenciesAndNumRows): FrequenciesAndNumRows = {

    val totalRows = numRows + other.numRows
    val newTable = FrequencyBasedOperatorsUtils.join(table, other.table)

    FrequenciesAndNumRows(newTable, Some(totalRows), Some(numNulls() + other.numNulls()))
  }

  def frequencies(): (JdbcStructType, Map[Seq[String], Long]) = {

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

    (table.schema(), frequencies)
  }
}

object FrequencyBasedOperatorsUtils {

  def uniqueTableName(): String = {
    s"__deequ__frequenciesAndNumRows_${
      UUID.randomUUID().toString
        .replace("-", "")
    }"
  }

  private[jdbc] def join(first: Table,
                         second: Table): Table = {

    val table = Table(uniqueTableName(), first.jdbcConnection)
    val columns = first.schema()
    val groupingColumns = columns.columnsNamesAsSeq().filter(col => col != Operators.COUNT_COL)

    if (columns.columnsNamesAsSet() == second.schema().columnsNamesAsSet()) {

      val query =
        s"""
           |CREATE TEMPORARY TABLE
           | ${table.name}
           |AS
           | SELECT
           |  ${groupingColumns.mkString(", ")},
           |  SUM(${Operators.COUNT_COL}) as ${Operators.COUNT_COL}
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

object FrequenciesAndNumRows {

  def from(connection: Connection,
           schema: JdbcStructType,
           frequencies: Map[Seq[String], Long], numRows: Long): FrequenciesAndNumRows = {

    val table = Table(FrequencyBasedOperatorsUtils.uniqueTableName(), connection, temporary = true)
    Table.createAndFill(table, schema, frequencies)

    FrequenciesAndNumRows(table, Some(numRows))
  }
}
