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

import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasNoInjection}
import com.amazon.deequ.analyzers.runners.{EmptyStateException, MetricCalculationException}
import com.amazon.deequ.analyzers.{DataTypeHistogram, DataTypeInstances}
import com.amazon.deequ.metrics.HistogramMetric

import scala.util.{Failure, Success}

case class JdbcDataType(
                         column: String,
                         where: Option[String] = None)
  extends JdbcScanShareableAnalyzer[DataTypeHistogram, HistogramMetric] {

  override def aggregationFunctions(): Seq[String] = {
    /*
     * Scan the column and, for each supported data type (unknown, integral, fractional,
     * boolean, string), aggregate the number of values that could be instances of the
     * respective data type. Each value is assigned to exactly one data type (i.e., data
     * types are mutually exclusive).
     * A value's data type is assumed to be unknown, if it is a NULL-value.
     * A value is integral if it contains only digits without any fractional part.
     * A value is fractional if it contains only digits and a fractional part separated by
     * a decimal separator.
     * A value is boolean if it is either true or false.
    */
    // use triple quotes to avoid special escaping
    val integerPattern = """^(-|\+)? ?\d*$"""
    val fractionPattern = """^(-|\+)? ?\d*\.\d*$"""
    val booleanPattern = """^(true|false)$"""

    def countOccurrencesOf(pattern: String): String = {
      def toString(col: String) = s"CAST($col AS TEXT)"

      s"COUNT(${
        conditionalSelection(column,
          Some(s"(SELECT regexp_matches(${toString(column)}, '$pattern', '')) IS NOT NULL") ::
            where :: Nil)
      })"
    }

    s"COUNT(${conditionalSelection(column, where)})" :: conditionalCount(where) ::
      countOccurrencesOf(integerPattern) :: countOccurrencesOf(fractionPattern) ::
      countOccurrencesOf(booleanPattern) :: s"MIN($column)" :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): Option[DataTypeHistogram] = {
    ifNoNullsIn(result, offset, 6) { _ =>
      // column at offset + 5 contains minimum value of the column
      val dataType = result.row(offset + 5) match {
        case _: Integer | _: Byte | _: Short | _: Long => DataTypeInstances.Integral
        case _: Boolean => DataTypeInstances.Boolean
        case _: Double | _: Float => DataTypeInstances.Fractional
        case _ => DataTypeInstances.Unknown
      }

      if (dataType != DataTypeInstances.Unknown) {
        val numNotNulls = result.getLong(offset)
        val numRows = result.getLong(offset + 1)
        DataTypeHistogram(
          numNull = numRows - numNotNulls,
          numFractional = if (dataType == DataTypeInstances.Fractional) numNotNulls else 0,
          numIntegral = if (dataType == DataTypeInstances.Integral) numNotNulls else 0,
          numBoolean = if (dataType == DataTypeInstances.Boolean) numNotNulls else 0,
          numString = 0
        )
      } else {
        val numNotNulls = result.getLong(offset)
        val numRows = result.getLong(offset + 1)
        val numIntegers = result.getLong(offset + 2)
        val numFractions = result.getLong(offset + 3)
        val numBooleans = result.getLong(offset + 4)

        DataTypeHistogram(
          numNull = numRows - numNotNulls,
          numFractional = numFractions,
          numIntegral = numIntegers,
          numBoolean = numBooleans,
          numString = numRows - (numRows - numNotNulls) - numBooleans - numIntegers - numFractions
        )
      }
    }
  }

  override def computeMetricFrom(state: Option[DataTypeHistogram]): HistogramMetric = {
    state match {
      case Some(histogram) =>
        HistogramMetric(column, Success(DataTypeHistogram.toDistribution(histogram)))
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcDataType, all input values were NULL."))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def additionalPreconditions: Seq[Table => Unit] = {
    hasColumn(column) :: hasNoInjection(where) :: Nil
  }
}
