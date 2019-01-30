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

package com.amazon.deequ.analyzers

import java.sql.{Connection, ResultSet}

import com.amazon.deequ.analyzers.Analyzers.{COUNT_COL, _}
import com.amazon.deequ.analyzers.Preconditions._
import com.amazon.deequ.analyzers.jdbc.{JdbcFrequencyBasedAnalyzerUtils, JdbcPreconditions, Table}
import com.amazon.deequ.metrics.DoubleMetric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class FrequencyBasedAnalyzer(columnsToGroupOn: Seq[String])
  extends GroupingAnalyzer[FrequenciesAndNumRows, DoubleMetric] {

  override def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(data: DataFrame): Option[FrequenciesAndNumRows] = {
    Some(FrequencyBasedAnalyzer.computeFrequencies(data, groupingColumns()))
  }

  override def computeStateFrom(table: Table): Option[FrequenciesAndNumRows] = {
    Some(FrequencyBasedAnalyzer.computeFrequencies(table, groupingColumns()))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditionsWithSpark: Seq[StructType => Unit] = {
    super.preconditionsWithSpark ++ Seq(atLeastOne(columnsToGroupOn)) ++ columnsToGroupOn.map { hasColumn }
  }

  override def preconditionsWithJdbc: Seq[Table => Unit] = {
    super.preconditionsWithJdbc ++ Seq(JdbcPreconditions.atLeastOne(columnsToGroupOn)) ++ columnsToGroupOn.map { JdbcPreconditions.hasColumn }
  }
}

object FrequencyBasedAnalyzer {

  def computeFrequencies(data: Any,
                         groupingColumns: Seq[String],
                         numRows: Option[Long] = None): FrequenciesAndNumRows = {

    data match {
      case df: DataFrame => computeFrequenciesWithSpark(df, groupingColumns, numRows)
      case tbl: Table => computeFrequenciesWithJdbc(tbl, groupingColumns, numRows)

      case _ => throw IllegalDataFormatException()
    }
  }

  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequenciesWithSpark(
      data: DataFrame,
      groupingColumns: Seq[String],
      numRows: Option[Long] = None)
    : FrequenciesAndNumRows = {

    val columnsToGroupBy = groupingColumns.map { name => col(name) }.toArray
    val projectionColumns = columnsToGroupBy :+ col(COUNT_COL)

    val noGroupingColumnIsNull = groupingColumns
      .foldLeft(expr(true.toString)) { case (condition, name) =>
        condition.and(col(name).isNotNull)
      }

    val frequencies = data
      .select(columnsToGroupBy: _*)
      .where(noGroupingColumnIsNull)
      .groupBy(columnsToGroupBy: _*)
      .agg(count(lit(1)).alias(COUNT_COL))
      .select(projectionColumns: _*)

    val numRowsOfData = numRows match {
      case Some(count) => count
      case _ => data.count()
    }

    FrequenciesAndNumRowsWithSpark(frequencies, numRowsOfData)
  }

  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequenciesWithJdbc(
                          table: Table,
                          groupingColumns: Seq[String],
                          numRows: Option[Long] = None)
  : FrequenciesAndNumRowsWithJdbc = {

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
         | SELECT $selectColumns, COUNT(*) AS $COUNT_COL
         |    FROM ${table.name}
         |    GROUP BY $groupByColumns
        """.stripMargin

    val statement = connection.createStatement()
    statement.execute(query)

    FrequenciesAndNumRowsWithJdbc(frequenciesTable, numRows)
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class ScanShareableFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends FrequencyBasedAnalyzer(columnsToGroupOn) {

  def aggregationFunctionsWithSpark(numRows: Long): Seq[Column]
  def aggregationFunctionsWithJdbc(numRows: Long): Seq[String]

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>

        theState match {
          case sparkState: FrequenciesAndNumRowsWithSpark =>
            val aggregations = aggregationFunctionsWithSpark(sparkState.numRows)

            val result = sparkState.frequencies.agg(aggregations.head, aggregations.tail: _*).collect()
              .head

            fromAggregationResult(AggregationResult.from(result), 0)

          case jdbcState: FrequenciesAndNumRowsWithJdbc =>
            val aggregations = aggregationFunctionsWithJdbc(jdbcState.numRows())

            val result = jdbcState.table.executeAggregations(aggregations)

            fromAggregationResult(result, 0)
        }

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

  def fromAggregationResult(result: AggregationResult, offset: Int): DoubleMetric = {
    if (result.isNullAt(offset)) {
      emptyFailureMetric()
    } else {
      toSuccessMetric(result.getDouble(offset))
    }
  }
}
