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

/**
  * Distinctness is the fraction of distinct values of a column(s).
  *
  * @param columns  the column(s) for which to compute distinctness
  */
case class DistinctnessOp(columns: Seq[String])
  extends ScanShareableFrequencyBasedOperator("Distinctness", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {

    val condition = Some(s"${columns.head} IS NOT NULL")
    val count = s"SUM(${conditionalSelection("1", condition)})"

    s"(${toDouble(count)} / $numRows)" :: Nil
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        if (theState.numNulls() == theState.numRows) {
          return metricFromEmpty(this, "Uniqueness",
            columns.mkString(","), entityFrom(columns))
        }
      case None =>
    }
    super.computeMetricFrom(state)
  }
}

object DistinctnessOp {
  def apply(column: String): DistinctnessOp = {
    new DistinctnessOp(column :: Nil)
  }
}
