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

package com.amazon.deequ.runtime.jdbc.operators

import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.runtime.jdbc.operators.Operators._

case class UniqueValueRatioOp(columns: Seq[String])
  extends ScanShareableFrequencyBasedOperator("UniqueValueRatio", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {
    val noNullValue = Some(s"${columns.head} IS NOT NULL")
    val conditions = noNullValue :: Some(s"${Operators.COUNT_COL} = 1") :: Nil

    s"SUM(${conditionalSelection("1", conditions)})" ::
      conditionalCount(noNullValue) :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    if (result.getDouble(offset + 1) == 0) {
      emptyFailureMetric()
    } else {
      val numUniqueValues = result.getDouble(offset)
      val numDistinctValues = result.getLong(offset + 1).toDouble

      toSuccessMetric(numUniqueValues / numDistinctValues)
    }
  }
}

object UniqueValueRatioOp {
  def apply(column: String): UniqueValueRatioOp = {
    new UniqueValueRatioOp(column :: Nil)
  }
}
