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

import java.sql.ResultSet

import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import com.amazon.deequ.runtime.jdbc.executor._

import scala.collection.mutable
import scala.util.{Failure, Try}

/**
  * Histogram is the summary of values in a column of a DataFrame. Groups the given column's values,
  * and calculates the number of rows with that specific value and the fraction of this value.
  *
  * @param column        Column to do histogram analysis on
  * @param binningUdf    Optional binning function to run before grouping to re-categorize the
  *                      column values.
  *                      For example to turn a numerical value to a categorical value binning
  *                      functions might be used.
  * @param maxDetailBins Histogram details is only provided for N column values with top counts.
  *                      maxBins sets the N.
  *                      This limit does not affect what is being returned as number of bins. It
  *                      always returns the dictinct value count.
  */
case class HistogramOp(
    column: String,
    binningUdf: Option[Any => Any] = None,
    maxDetailBins: Integer = HistogramOp.MaximumAllowedDetailBins)
  extends Operator[FrequenciesAndNumRows, HistogramMetric] {

  private[this] val PARAM_CHECK: Table => Unit = { _ =>
    if (maxDetailBins > HistogramOp.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
        s"than ${HistogramOp.MaximumAllowedDetailBins} values")
    }
  }

  override def computeStateFrom(table: Table): Option[FrequenciesAndNumRows] = {

    val query =
      s"""
         | SELECT $column as name, COUNT(*) AS ${Operators.COUNT_COL}
         |    FROM ${table.name}
         |    GROUP BY $column
        """.stripMargin

    val statement = table.jdbcConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()
    val metaData = result.getMetaData

    val colCount = metaData.getColumnCount

    var cols = mutable.LinkedHashMap[String, JdbcDataType]()
    for (col <- 1 to colCount) {
      cols(metaData.getColumnLabel(col)) = StringType
    }

    def convertResult(resultSet: ResultSet,
                      map: Map[Seq[String], Long],
                      total: Long): (Map[Seq[String], Long], Long) = {
      if (result.next()) {
        val distinctName = result.getObject("name")

        val modifiedName = binningUdf match {
          case Some(bin) => bin(distinctName)
          case _ => distinctName
        }

        val discreteValue = modifiedName match {
          case null => Seq[String](HistogramOp.NullFieldReplacement)
          case _ => Seq[String](modifiedName.toString)
        }

        val absolute = result.getLong(s"${Operators.COUNT_COL}")

        val frequency = map.getOrElse(discreteValue, 0L) + absolute
        val entry = discreteValue -> frequency
        convertResult(result, map + entry, total + absolute)
      } else {
        (map, total)
      }
    }

    val frequenciesAndNumRows = convertResult(result, Map[Seq[String], Long](), 0)
    val frequencies = frequenciesAndNumRows._1
    val numRows = frequenciesAndNumRows._2

    result.close()
    Some(FrequenciesAndNumRows.from(table.jdbcConnection, cols, frequencies, numRows))
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): HistogramMetric = {

    state match {

      case Some(theState) =>
        val value: Try[Distribution] = Try {

          val topNFreq = topNFrequencies(theState.frequencies()._2, maxDetailBins)
          val binCount = theState.frequencies()._2.size

          val histogramDetails = topNFreq.keys
            .map { discreteValue: Seq[String] =>
              val absolute = theState.frequencies()._2(discreteValue)
              val ratio = absolute.toDouble / theState.numRows
              discreteValue.head -> DistributionValue(absolute, ratio)
            }
            .toMap

          Distribution(histogramDetails, binCount)
        }

        HistogramMetric(column, value)

      case None =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCompleteness, all input values were NULL."))
    }
  }

  /**
    * Receive the top n key-value-pairs of a map with respect to the value.
    *
    * @param frequencies  Maps data to their occurrences.
    * @param n            The number of maximal returned frequencies.
    * @return             Biggest n key-value-pairs of frequencies with respect to the value.
    */
  def topNFrequencies(frequencies: Map[Seq[String], Long], n: Int) : Map[Seq[String], Long] = {
    if (frequencies.size <= n) {
      return frequencies
    }

    frequencies.foldLeft(Map[Seq[String], Long]()) {
      (top, i) =>
        if (top.size < n) {
          top + i
        } else if (top.minBy(_._2)._2 < i._2) {
          top - top.minBy(_._2)._1 + i
        } else {
          top
        }
    }
  }

  override def toFailureMetric(exception: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[Table => Unit] = {
    PARAM_CHECK :: Preconditions.hasTable() :: Preconditions.hasColumn(column) :: Nil
  }
}

object HistogramOp {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
}

object OrderByAbsoluteCount extends Ordering[JdbcRow] {
  override def compare(x: JdbcRow, y: JdbcRow): Int = {
    x.getLong(1).compareTo(y.getLong(1))
  }
}
