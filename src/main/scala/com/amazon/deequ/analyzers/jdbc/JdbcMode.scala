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
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}

import scala.util.{Failure, Success, Try}

case class ModeMetric(columns: Seq[String], value: Try[Tuple3[String, Double, Double]])
  extends Metric[Tuple3[String, Double, Double]] {
  val entity: Entity.Value = entityFrom(columns)
  val instance: String = columns.mkString(",")
  val name = "Mode"

  def flatten(): Seq[DoubleMetric] = {
    value
      .map { tuple =>
        val frequency = DoubleMetric(entity, s"$name.frequency_for: ${tuple._1}", instance,
          Success(tuple._2))

        val ratio = DoubleMetric(entity, s"$name.ratio", instance, Success(tuple._3))
        Seq(frequency, ratio)
      }
      .recover {
        case e: Exception => Seq(metricFromFailure(e, name, instance, entity))
      }
      .get
  }
}

/**
  * Mode is the most frequent combination of values occurring in the specified columns. If it is
  * not unique, an arbitrary candidate is selected.
  */
case class JdbcMode(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("Mode", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {
    val noNullColumns = columns.map(column => s"$column IS NOT NULL").mkString(" AND ")
    val mode = columns.map(column => s"$column::text").mkString(" || ', ' || ")
    val paddedFrequency = s"LPAD(${Analyzers.COUNT_COL}::text, char_length($numRows::text), '0')"
    s"MAX(CASE WHEN $noNullColumns THEN $paddedFrequency || '|' || $mode ELSE NULL END)" ::
      s"$numRows" :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): ModeMetric = {
    result.getMode(offset) match {
      case Some(mode) => ModeMetric(columns, Success(mode))
      case _ => ModeMetric(columns, Failure(emptyStateException(this)))
    }
  }
}
