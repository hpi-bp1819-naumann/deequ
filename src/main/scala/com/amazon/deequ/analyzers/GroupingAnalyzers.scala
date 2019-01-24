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
    Seq(atLeastOne(columnsToGroupOn)) ++ columnsToGroupOn.map { hasColumn } ++ super.preconditionsWithSpark
  }

  override def preconditionsWithJdbc: Seq[Table => Unit] = {
    Seq(JdbcPreconditions.atLeastOne(columnsToGroupOn)) ++ columnsToGroupOn.map { JdbcPreconditions.hasColumn } ++ super.preconditionsWithJdbc
  }
}

object FrequencyBasedAnalyzer {

  def computeFrequencies(data: Any,
                         groupingColumns: Seq[String],
                         numRows: Option[Long] = None): FrequenciesAndNumRows = {

    data match {
      case df: DataFrame => computeFrequenciesWithSpark(df, groupingColumns, numRows)
      case tbl: Table => computeFrequenciesWithJdbc(tbl, groupingColumns, numRows)

      case _ => throw new IllegalArgumentException("data can only be of type DataFrame or Table")
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

    val defaultTable = JdbcFrequencyBasedAnalyzerUtils.newDefaultTable()

    //TODO
/*
    if (table.jdbcUrl == defaultTable.jdbcUrl) {
      return computeFrequenciesInSourceDb(table, groupingColumns, numRows, defaultTable)
    }*/

    var cols = mutable.LinkedHashMap[String, String]()
    var result: ResultSet = null

    table.withJdbc { connection: Connection =>

      val columns = groupingColumns.mkString(", ")
      val query =
        s"""
           | SELECT $columns, COUNT(*) AS absolute
           |    FROM ${table.name}
           |    GROUP BY $columns
          """.stripMargin

      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)

      result = statement.executeQuery()

      for (col <- groupingColumns) {
        cols(col) = "TEXT"
      }
      cols("absolute") = "BIGINT"

      val targetTable = Table.create(defaultTable, cols)


      /**
        *
        * @param connection used to write frequencies into targetTable
        * @param result     grouped frequencies
        * @param total      overall number of rows
        * @param numNulls   number of rows with at least one column with a null value
        * @return (total, numNulls)
        */
      def convertResultSet(targetConnection: Connection,
                           result: ResultSet,
                           total: Long, numNulls: Long): (Long, Long) = {

        if (result.next()) {
          var columnValues = Seq[String]()
          val absolute = result.getLong("absolute")

          // only make a map entry if the value is defined for all columns
          for (i <- 1 to groupingColumns.size) {
            val columnValue = Option(result.getObject(i))
            columnValue match {
              case Some(theColumnValue) =>
                columnValues = columnValues :+ theColumnValue.toString
              case None =>
                return convertResultSet(targetConnection, result,
                  total + absolute, numNulls + absolute)
            }
          }

          val query =
            s"""
               | INSERT INTO
               |  ${targetTable.name} (
               |    ${cols.keys.toSeq.mkString(", ")})
               | VALUES
               |  (${columnValues.mkString("'", "', '", "'")}, $absolute)
            """.stripMargin

          val statement = targetConnection.createStatement()
          statement.execute(query)

          convertResultSet(targetConnection, result, total + absolute, numNulls)
        } else {
          (total, numNulls)
        }
      }


      targetTable.withJdbc[FrequenciesAndNumRowsWithJdbc] { targetConnection: Connection =>

        val (numRows, numNulls) = convertResultSet(targetConnection, result, 0, 0)

        val nullValueQuery =
          s"""
             | INSERT INTO
             |  ${targetTable.name} (
             |  ${cols.keys.toSeq.mkString(", ")})
             | VALUES
             |  (${Seq.fill(groupingColumns.size)("null").mkString(", ")}, $numNulls)
        """.stripMargin

        val nullValueStatement = targetConnection.createStatement()
        nullValueStatement.execute(nullValueQuery)

        result.close()

        FrequenciesAndNumRowsWithJdbc(targetTable, Some(numRows), Some(numNulls))
      }
    }
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

  def fromAggregationResult(result: AggregationResult, offset: Int): DoubleMetric = {
    if (result.isNullAt(offset)) {
      metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    } else {
      toSuccessMetric(result.getDouble(offset))
    }
  }
}
