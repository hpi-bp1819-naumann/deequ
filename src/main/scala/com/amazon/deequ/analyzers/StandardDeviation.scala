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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzers, JdbcPreconditions, Table}
import org.apache.spark.sql.DeequFunctions.stateful_stddev_pop
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}

case class StandardDeviationState(
    n: Double,
    avg: Double,
    m2: Double)
  extends DoubleValuedState[StandardDeviationState] {

  require(n > 0.0, "Standard deviation is undefined for n = 0.")

  override def metricValue(): Double = {
    math.sqrt(m2 / n)
  }

  override def sum(other: StandardDeviationState): StandardDeviationState = {
    val newN = n + other.n
    val delta = other.avg - avg
    val deltaN = if (newN == 0.0) 0.0 else delta / newN

    StandardDeviationState(newN, avg + deltaN * other.n,
      m2 + other.m2 + delta * deltaN * n * other.n)
  }
}

case class StandardDeviation(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[StandardDeviationState]("StandardDeviation", column) {

  override def aggregationFunctionsWithSpark(): Seq[Column] = {
    stateful_stddev_pop(conditionalSelection(column, where)) :: Nil
  }

  override def aggregationFunctionsWithJdbc() : Seq[String] = {
    JdbcAnalyzers.conditionalCountNotNull(column, where) :: s"SUM(${JdbcAnalyzers.conditionalSelection(column, where)})" ::
      s"SUM(POWER(${JdbcAnalyzers.conditionalSelection(column, where)}, 2))" :: Nil
  }

  override def fromAggregationResult(result: AggregationResult, offset: Int): Option[StandardDeviationState] = {

    if (result.row.size == 1) {
      if (result.isNullAt(offset)) {
        None
      } else {
        val row = result.getAs[Row](offset)
        val n = row.getDouble(0)

        if (n == 0.0) {
          None
        } else {
          Some(StandardDeviationState(n, row.getDouble(1), row.getDouble(2)))
        }
      }
    } else {
      ifNoNullsIn(result, offset, 3) { _ =>
        val num_rows = result.getDouble(offset)
        val col_sum = result.getDouble(offset + 1)
        val col_sum_squared = result.getDouble(offset + 2)
        val col_avg : Double = col_sum / num_rows
        val m2 : Double = col_sum_squared - col_sum * col_sum / num_rows
        StandardDeviationState(num_rows, col_avg, m2)
      }
    }
  }

  override protected def additionalPreconditionsWithSpark(): Seq[StructType => Unit] = {
    Preconditions.hasColumn(column) :: Preconditions.isNumeric(column) :: Nil
  }

  override def additionalPreconditionsWithJdbc(): Seq[Table => Unit] = {
    JdbcPreconditions.hasColumn(column) :: JdbcPreconditions.isNumeric(column) ::
      JdbcPreconditions.hasNoInjection(where) :: Nil
  }
}
