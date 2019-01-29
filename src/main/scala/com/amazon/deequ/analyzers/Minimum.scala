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
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.types.{DoubleType, StructType}

case class MinState(minValue: Double) extends DoubleValuedState[MinState] {

  override def sum(other: MinState): MinState = {
    MinState(math.min(minValue, other.minValue))
  }

  override def metricValue(): Double = {
    minValue
  }
}

case class Minimum(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MinState]("Minimum", column) {

  override def aggregationFunctionsWithSpark(): Seq[Column] = {
    min(Analyzers.conditionalSelection(column, where)).cast(DoubleType) :: Nil
  }

  override def aggregationFunctionsWithJdbc(): Seq[String] = {
    s"MIN(${JdbcAnalyzers.conditionalSelection(column, where)})" :: Nil
  }

  override def fromAggregationResult(result: AggregationResult, offset: Int): Option[MinState] = {

    ifNoNullsIn(result, offset) { _ =>
      MinState(result.getDouble(offset))
    }
  }

  override protected def additionalPreconditionsWithSpark(): Seq[StructType => Unit] = {
    Preconditions.hasColumn(column) :: Preconditions.isNumeric(column) :: Nil
  }

  override protected def additionalPreconditionsWithJdbc(): Seq[Table => Unit] = {
    JdbcPreconditions.hasColumn(column) :: JdbcPreconditions.isNumeric(column) ::
      JdbcPreconditions.hasNoInjection(where) :: Nil
  }
}
