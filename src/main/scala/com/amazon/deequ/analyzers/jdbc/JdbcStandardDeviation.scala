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
import com.amazon.deequ.analyzers.StandardDeviationState
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable, isNumeric, hasNoInjection}
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}

case class JdbcStandardDeviation(column: String, where: Option[String] = None)
  extends JdbcAnalyzer[StandardDeviationState, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: isNumeric(column) :: hasNoInjection(where) :: Nil
  }

  override def computeStateFrom(table: Table): Option[StandardDeviationState] = {

    val connection = table.jdbcConnection
    val query =
      s"""
         |SELECT
         |  COUNT(*) AS num_rows,
         |  SUM($column) AS sum, SUM(POWER($column, 2)) AS sum2
         | FROM
         |  ${table.name}
         | WHERE
         |  ${where.getOrElse("TRUE=TRUE")}
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    if (result.next()) {
      val num_rows = result.getDouble(1)
      val sum = result.getDouble(2)
      val sum2 = result.getDouble(3)

      if (num_rows > 0) {
        result.close()
        return Some(StandardDeviationState(num_rows, sum/num_rows, sum2 - sum*sum/num_rows))
      }
    }
    result.close()
    None
  }

  override def computeMetricFrom(state: Option[StandardDeviationState]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "StandardDeviation", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcStandardDeviation, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "StandardDeviation", column, Entity.Column)
  }
}
