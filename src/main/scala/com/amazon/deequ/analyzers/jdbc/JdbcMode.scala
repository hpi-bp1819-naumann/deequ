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
import com.amazon.deequ.analyzers.jdbc.Preconditions._
import com.amazon.deequ.metrics.{DoubleMetric, Entity}

import scala.util.Success

/**
  * Mode is the most frequent value occurring in the specified column. If it is not unique, an
  * arbitrary candidate is selected. The mode analyzer only works for numeric columns.
  */
case class JdbcMode(column: String)
  extends JdbcScanShareableFrequencyBasedAnalyzer("Mode", Seq(column)) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {
    val mode = s"CAST($column AS TEXT)"
    val frequency = s"CAST(${Analyzers.COUNT_COL} AS TEXT)"
    val paddedFrequency = s"LPAD($frequency, char_length('$numRows'), '0')"
    s"MAX(CASE WHEN $column IS NOT NULL THEN $paddedFrequency || '|' || $mode ELSE NULL END)" :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    result.getMode(offset) match {
      case Some(mode) => DoubleMetric(Entity.Column, s"Mode", column, Success(mode))
      case _ => emptyFailureMetric()
    }
  }

  override def preconditions: Seq[Table => Unit] = {
    super.preconditions ++ Seq(isNumeric(column))
  }
}
