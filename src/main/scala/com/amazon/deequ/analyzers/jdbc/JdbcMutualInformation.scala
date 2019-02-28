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
  * Mutual Information describes how much information about one column can be inferred from another
  * column.
  *
  * If two columns are independent of each other, then nothing can be inferred from one column about
  * the other, and mutual information is zero. If there is a functional dependency of one column to
  * another and vice versa, then all information of the two columns are shared, and mutual
  * information is the entropy of each column.
  */
case class JdbcMutualInformation(columns: Seq[String])
  extends JdbcFrequencyBasedAnalyzer("MutualInformation", columns) {

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {
    state match {
      case Some(theState) =>
        val total = theState.numRows()
        val Seq(col1, col2) = columns

        val freqCol1 = s"__deequ_f1_$col1"
        val freqCol2 = s"__deequ_f2_$col2"

        val jointStats = theState.table.name

        val marginalStats1 =
          s"""
             |SELECT
             |  $col1, ${toDouble(s"SUM(${Analyzers.COUNT_COL})")} AS $freqCol1
             |FROM
             |  $jointStats
             |GROUP BY
             |  $col1
          """.stripMargin

        val marginalStats2 =
          s"""
             |SELECT
             |  $col2, ${toDouble(s"SUM(${Analyzers.COUNT_COL})")} AS $freqCol2
             |FROM
             |  $jointStats
             |GROUP BY
             |  $col2
          """.stripMargin

        def miExpr(px: String, py: String, pxy: String): String =
          s"($pxy / CAST($total AS DOUBLE PRECISION)) * " +
            s"ln(($pxy / CAST($total AS DOUBLE PRECISION)) / " +
            s"(($px / CAST($total AS DOUBLE PRECISION)) * " +
            s"($py / CAST($total AS DOUBLE PRECISION))))"

        val value =
          s"""
             |SELECT
             |  SUM(${miExpr(freqCol1, freqCol2, Analyzers.COUNT_COL)})
             |FROM
             |  $jointStats, ($marginalStats1) m1, ($marginalStats2) m2
             |WHERE
             |  $jointStats.$col1 = m1.$col1 AND $jointStats.$col2 = m2.$col2
           """.stripMargin

        val result = theState.table.jdbcConnection.createStatement().executeQuery(value)

        if (result.next()) {
          val resultRow = JdbcRow.from(result)
          result.close()

          if (resultRow.isNullAt(0)) {
            emptyFailureMetric()
          } else {
            toSuccessMetric(resultRow.getDouble(0))
          }
        } else {
          emptyFailureMetric()
        }
      case None =>
        emptyFailureMetric()
    }
  }

  /** Mutual information is defined for exactly two columns */
  override def preconditions: Seq[Table => Unit] = {
    super.preconditions ++ Seq(Preconditions.exactlyNColumns(columns, 2))
  }
}

object JdbcMutualInformation {
  def apply(columnA: String, columnB: String): JdbcMutualInformation = {
    JdbcMutualInformation(columnA :: columnB :: Nil)
  }
}
