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

import java.nio.ByteBuffer

import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import com.amazon.deequ.runtime.jdbc.executor.{EmptyStateException, MetricCalculationException}
import com.amazon.deequ.runtime.jdbc.operators.Operators._
import com.amazon.deequ.runtime.jdbc.operators.Preconditions._
import com.amazon.deequ.statistics.DataTypeInstances

import scala.util.{Failure, Success}

case class DataTypeHistogram(
    numNull: Long,
    numFractional: Long,
    numIntegral: Long,
    numBoolean: Long,
    numString: Long)
  extends State[DataTypeHistogram] {

  override def sum(other: DataTypeHistogram): DataTypeHistogram = {
    DataTypeHistogram(numNull + other.numNull, numFractional + other.numFractional,
      numIntegral + other.numIntegral, numBoolean + other.numBoolean, numString + other.numString)
  }
}

object DataTypeHistogram {

  val SIZE_IN_BYTES = 40
  private[deequ] val NULL_POS = 0
  private[deequ] val FRACTIONAL_POS = 1
  private[deequ] val INTEGRAL_POS = 2
  private[deequ] val BOOLEAN_POS = 3
  private[deequ] val STRING_POS = 4

  def fromBytes(bytes: Array[Byte]): DataTypeHistogram = {
    require(bytes.length == SIZE_IN_BYTES)
    val buffer = ByteBuffer.wrap(bytes).asLongBuffer().asReadOnlyBuffer()
    val numNull = buffer.get(NULL_POS)
    val numFractional = buffer.get(FRACTIONAL_POS)
    val numIntegral = buffer.get(INTEGRAL_POS)
    val numBoolean = buffer.get(BOOLEAN_POS)
    val numString = buffer.get(STRING_POS)

    DataTypeHistogram(numNull, numFractional, numIntegral, numBoolean, numString)
  }

  def toBytes(
      numNull: Long,
      numFractional: Long,
      numIntegral: Long,
      numBoolean: Long,
      numString: Long)
    : Array[Byte] = {

    val out = ByteBuffer.allocate(SIZE_IN_BYTES)
    val outB = out.asLongBuffer()

    outB.put(numNull)
    outB.put(numFractional)
    outB.put(numIntegral)
    outB.put(numBoolean)
    outB.put(numString)

    // TODO avoid allocation
    val bytes = new Array[Byte](out.remaining)
    out.get(bytes)
    bytes
  }

  def toDistribution(hist: DataTypeHistogram): Distribution = {
    val totalObservations =
      hist.numNull + hist.numString + hist.numBoolean + hist.numIntegral + hist.numFractional

    Distribution(Map(
      DataTypeInstances.Unknown.toString ->
        DistributionValue(hist.numNull, hist.numNull.toDouble / totalObservations),
      DataTypeInstances.Fractional.toString ->
        DistributionValue(hist.numFractional, hist.numFractional.toDouble / totalObservations),
      DataTypeInstances.Integral.toString ->
        DistributionValue(hist.numIntegral, hist.numIntegral.toDouble / totalObservations),
      DataTypeInstances.Boolean.toString ->
        DistributionValue(hist.numBoolean, hist.numBoolean.toDouble / totalObservations),
      DataTypeInstances.String.toString ->
        DistributionValue(hist.numString, hist.numString.toDouble / totalObservations)),
      numberOfBins = 5)
  }

  def determineType(dist: Distribution): DataTypeInstances.Value = {

    import DataTypeInstances._

    // If all are unknown, we can't decide
    if (ratioOf(Unknown, dist) == 1.0) {
      Unknown
    } else {
      // If we saw string values or a mix of boolean and numbers, we decide for String
      if (ratioOf(String, dist) > 0.0 ||
        (ratioOf(Boolean, dist) > 0.0 &&
          (ratioOf(Integral, dist) > 0.0 || ratioOf(Fractional, dist) > 0.0))) {
        String
      } else {
        // If we have boolean (but no numbers, because we checked for that), we go with boolean
        if (ratioOf(Boolean, dist) > 0.0) {
          Boolean
        } else {
          // If we have seen one fractional, we go with that type
          if (ratioOf(Fractional, dist) > 0.0) {
            Fractional
          } else {
            Integral
          }
        }
      }
    }
  }

  private[this] def ratioOf(key: DataTypeInstances.Value, distribution: Distribution): Double = {
    distribution.values
      .getOrElse(key.toString, DistributionValue(0L, 0.0))
      .ratio
  }
}

case class DataTypeOp(
    column: String,
    where: Option[String] = None)
  extends ScanShareableOperator[DataTypeHistogram, HistogramMetric] {

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
      def toString(col: String) = s"CASE WHEN $col IS NULL THEN '' ELSE CAST($col AS TEXT) END"

      s"COUNT(${
        conditionalSelection(column,
          Some(s"(SELECT regexp_matches(${toString(column)}, '$pattern', '')) IS NOT NULL") ::
            where :: Nil)
      })"
    }

    s"COUNT(${conditionalSelection(column, where)})" :: conditionalCount(where) ::
      countOccurrencesOf(integerPattern) :: countOccurrencesOf(fractionPattern) ::
      countOccurrencesOf(booleanPattern) :: s"CASE WHEN MIN($column) IS NULL THEN 0 ELSE MIN($column) END" :: Nil
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

  override def preconditions: Seq[Table => Unit] = {
    super.preconditions :+ hasColumn(column) :+ hasNoInjection(where)
  }
}
