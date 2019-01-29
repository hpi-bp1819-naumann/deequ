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
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzers, JdbcPreconditions, Table}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

case class MeanState(sum: Double, count: Long) extends DoubleValuedState[MeanState] {

  override def sum(other: MeanState): MeanState = {
    MeanState(sum + other.sum, count + other.count)
  }

  override def metricValue(): Double = {
    if (count == 0L) Double.NaN else sum / count
  }
}

case class Mean(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MeanState]("Mean", column) {

  override def aggregationFunctionsWithSpark(): Seq[Column] = {
    sum(Analyzers.conditionalSelection(column, where)).cast(DoubleType) ::
      count(Analyzers.conditionalSelection(column, where)).cast(LongType) :: Nil
  }

  override def aggregationFunctionsWithJdbc(): Seq[String] = {
    s"SUM(${JdbcAnalyzers.conditionalSelection(column, where)})" ::
      s"COUNT(${JdbcAnalyzers.conditionalSelection(column, where)})" :: Nil
  }

  override def fromAggregationResult(result: AggregationResult, offset: Int): Option[MeanState] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      MeanState(result.getDouble(offset), result.getLong(offset + 1))
    }
  }

  override protected def additionalPreconditionsWithSpark(): Seq[StructType => Unit] = {
    Preconditions.hasColumn(column) :: Preconditions.isNumeric(column) :: Nil
  }

  override protected def additionalPreconditionsWithJdbc(): Seq[Table => Unit] = {
    JdbcPreconditions.hasColumn(column) :: JdbcPreconditions.isNumeric(column) :: Nil
  }
}
