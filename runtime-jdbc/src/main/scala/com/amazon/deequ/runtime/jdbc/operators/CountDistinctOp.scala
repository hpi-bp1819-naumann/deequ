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

case class CountDistinctOp(columns: Seq[String])
  extends ScanShareableFrequencyBasedOperator("CountDistinct", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {

    val condition = Some(s"${columns.head} IS NOT NULL")
    s"COUNT(${conditionalSelection("1", condition)})" :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    toSuccessMetric(result.getLong(offset).toDouble)
  }
}

object CountDistinctOp {
  def apply(column: String): CountDistinctOp = {
    new CountDistinctOp(column :: Nil)
  }
}
