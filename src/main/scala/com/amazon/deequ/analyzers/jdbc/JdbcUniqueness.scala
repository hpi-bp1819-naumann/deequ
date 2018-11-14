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

import com.amazon.deequ.analyzers.Analyzers.{metricFromFailure, metricFromValue}
import com.amazon.deequ.analyzers.NumMatchesAndCount

import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable}
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException

case class JdbcUniqueness(column: String)
  extends JdbcAnalyzer[NumMatchesAndCount, DoubleMetric] {


  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[NumMatchesAndCount] = {
    val connection = table.jdbcConnection
    val query =
      s"""
       | SELECT *
       | FROM
       |
       | (SELECT COUNT(*) as num_unique_values
       | FROM
       |    (SELECT $column,
       |    COUNT(*) AS number_values
       |    FROM ${table.name}
       |    GROUP BY $column) AS grouping
       | WHERE number_values = 1) as filtered_grouping
       |
       | CROSS JOIN
       |
       |  (SELECT COUNT(*) AS num_rows
       |  FROM ${table.name}) AS _
       |
       |
      """.stripMargin


    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    if (result.next()) {
      val num_unique_values = result.getLong("num_unique_values")
      val num_rows = result.getLong("num_rows")
      val num_unique_values = result.getLong("num_unique_values")

      if (num_rows > 0) {
        return Some(NumMatchesAndCount(num_unique_values, num_rows))
      }
    }
    None
  }

  override def computeMetricFrom(state: Option[NumMatchesAndCount]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "Uniqueness", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcUniqueness all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "Uniqueness", column, Entity.Column)
  }
}
