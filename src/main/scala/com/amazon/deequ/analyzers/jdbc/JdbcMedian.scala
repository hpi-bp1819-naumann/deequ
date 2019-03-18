/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.Analyzers
import com.amazon.deequ.metrics.DoubleMetric

/**
  * The median is the "middle" value separating the higher half from the lower half. It is only
  * implemented for numerical columns.
  */
case class JdbcMedian(column: String)
  extends JdbcFrequencyBasedAnalyzer("Median", column :: Nil) {

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {
    state match {
      case Some(theState) =>
        val table = theState.table.name

        val query =
          s"""
             |SELECT
             |  CASE WHEN (num_rows % 2 = 0 AND next IS NOT NULL)
             |  THEN 0.5 * ($column + next)
             |  ELSE $column END
             |FROM (
             |  SELECT
             |    $column,
             |    SUM(${Analyzers.COUNT_COL}) OVER (ORDER BY $column ASC) AS running_sum,
             |    SUM(${Analyzers.COUNT_COL}) OVER () AS num_rows,
             |    LEAD($column) OVER (ORDER BY $column ASC) AS next
             |  FROM
             |    $table
             |  WHERE
             |    $column IS NOT NULL
             |  ORDER BY $column ASC) intermediate
             |WHERE
             |  intermediate.running_sum >= (num_rows + 1) / 2
             |LIMIT 1;
          """.stripMargin

        val result = theState.table.jdbcConnection.createStatement().executeQuery(query)

        if (result.next()) {
          val resultRow = JdbcRow.from(result)
          result.close()

          if (resultRow.isNullAt(0)) {
            emptyFailureMetric()
          } else {
            toSuccessMetric(resultRow.getDouble(0))
          }
        } else {
          result.close()
          emptyFailureMetric()
        }
      case None =>
        emptyFailureMetric()
    }
  }

  /** Median is only defined for numerical columns */
  override def preconditions: Seq[Table => Unit] = {
    super.preconditions ++ Seq(Preconditions.isNumeric(column))
  }
}
