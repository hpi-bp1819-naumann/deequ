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

import java.sql.ResultSet

import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable}
import com.amazon.deequ.analyzers.runners.{EmptyStateException, MetricCalculationException}
import com.amazon.deequ.metrics._

import scala.util.{Failure, Try}

case class JdbcHistogram(column: String)
  extends JdbcAnalyzer[JdbcFrequenciesAndNumRows, HistogramMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable(column) :: hasColumn(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[JdbcFrequenciesAndNumRows] = {

    val connection = table.jdbcConnection

    // TODO: resolve rounding error somehow
    val query =
      s"""
         | SELECT $column AS name, c AS absolute, CAST(c AS float) / CAST(total AS float) AS ratio
         | FROM
         |   (SELECT $column, COUNT($column) AS c
         |    FROM ${table.name}
         |    GROUP BY $column)
         |    AS aggregates
         |
         | CROSS JOIN
         |
         |   (SELECT COUNT($column) AS total
         |   FROM ${table.name})
         |   AS nr
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    try {
      def convertResult(resultSet: ResultSet,
                        map: Map[String, DistributionValue],
                        total: Long): (Map[String, DistributionValue], Long) = {
        if (result.next()) {
          val columnName = result.getString("name")
          println(columnName)
          val absolute = result.getLong("absolute")
          val ratio = result.getDouble("ratio")
          val entry = columnName -> DistributionValue(absolute, ratio)
          convertResult(result, map + entry, totals + absolute)
        } else {
          (map, total)
        }
      }
      val frequenciesAndNumRows = convertResult(result, Map[String, DistributionValue](), 0)
      val frequencies = frequenciesAndNumRows._1

      val distribution = Distribution(frequencies, frequencies.size)
      Some(JdbcFrequenciesAndNumRows(distribution, frequenciesAndNumRows._2))
    }
    catch {
      case error: Exception => throw error
    }
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): HistogramMetric = {
    state match {

      case Some(theState) =>
        val value: Try[Distribution] = Try {
          // TODO: think about sorting
          theState.frequencies
        }

        HistogramMetric(column, value)

      case None =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCompleteness, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(failure)))
  }
}
