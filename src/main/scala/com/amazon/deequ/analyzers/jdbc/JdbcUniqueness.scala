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

package com.amazon.deequ.analyzers.jdbc
import com.amazon.deequ.analyzers.Analyzers
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.metrics.DoubleMetric

/** Uniqueness is the fraction of unique values of a column(s), i.e.,
  * values that occur exactly once. */
case class JdbcUniqueness(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("Uniqueness", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {

    val conditions = Some(s"${columns.head} IS NOT NULL") ::
      Some(s"${Analyzers.COUNT_COL} = 1") :: Nil
    val count = s"COUNT(${conditionalSelection("1", conditions)})"

    s"(${toDouble(count)} / $numRows)" :: Nil
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        if (theState.numNulls() == theState.numRows()) {
          return emptyFailureMetric()
        }
      case None =>
    }
    super.computeMetricFrom(state)
  }
}

object JdbcUniqueness {
  def apply(column: String): JdbcUniqueness = {
    new JdbcUniqueness(column :: Nil)
  }
}
