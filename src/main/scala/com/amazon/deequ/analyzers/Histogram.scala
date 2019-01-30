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

import java.sql.ResultSet

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.jdbc.{JdbcFrequencyBasedAnalyzerUtils, JdbcPreconditions, Table}
import com.amazon.deequ.analyzers.runners.{IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

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
case class Histogram(
    column: String,
    binningUdf: Option[Any] = None,
    maxDetailBins: Integer = Histogram.MaximumAllowedDetailBins)
  extends Analyzer[FrequenciesAndNumRows, HistogramMetric] {

  private[this] val PARAM_CHECK: Any => Unit = { _ =>
    if (maxDetailBins > Histogram.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
        s"than ${Histogram.MaximumAllowedDetailBins} values")
    }
  }

  override def computeStateFrom(data: DataFrame): Option[FrequenciesAndNumRows] = {

    // TODO figure out a way to pass this in if its known before hand
    val totalCount = data.count()

    val frequencies = (binningUdf match {
      case Some(func) =>

        func match {
          case bin: UserDefinedFunction =>
            data.withColumn(column, bin(col(column)))

          case _ => throw new IllegalAnalyzerParameterException(
            "binningUdf for spark analyzer must be of type UserDefinedFunction")
        }
      case _ => data
    })
      .select(col(column).cast(StringType))
      .na.fill(Histogram.NullFieldReplacement)
      .groupBy(column)
      .count()

    Some(FrequenciesAndNumRowsWithSpark(frequencies, totalCount))
  }

  override def computeStateFrom(table: Table): Option[FrequenciesAndNumRows] = {

    val connection = table.jdbcConnection
    val frequenciesTable = Table(JdbcFrequencyBasedAnalyzerUtils.uniqueTableName(), connection)

    // TODO: validate user defined function
    /*
    val noQuoteString = s"""[a-zA-Z]*"""
    val singleQuoteString = s"""('$noQuoteString')"""
    val doubleQuoteString = s"""("$noQuoteString")"""
    val number = s"""[0-9]*"""
    val stringOrNumber = s"""($noQuoteString|$singleQuoteString|$doubleQuoteString|$number)"""

    val mathComp = s"(<|>|(<=)|(>=)|=)"
    val comp = s"(( )?$mathComp( )?)|( IS( NOT) )"

    val optionalElse = s"""( ELSE $stringOrNumber)?"""

    val r = s"""WHEN $stringOrNumber( )?$comp$stringOrNumber (THEN $stringOrNumber)*$optionalElse"""
    */

    //s"""WHEN (('|")?[a-z]*|[0-9]*('|")?)(( )?((<|>|<=|>=|=)( )?|[IS( NOT) )])(([a-z]*')|("[a-z]*")|[a-z]*|[0-9]*))? THEN (([a-z]*')|("[a-z]*")|[a-z]*|[0-9]*)( ELSE (([a-z]*')|("[a-z]*")|[a-z]*|[0-9]*))?"""

    val columnSelection = binningUdf match {
      case Some(theBin) => theBin match {
        case func: String => s"CASE $func END"
        case _ => throw new IllegalAnalyzerParameterException("Need String")
      }
      case None => s"$column"
    }

    val query =
      s"""
         |CREATE TABLE ${frequenciesTable.name} AS
         | SELECT $columnSelection AS $column, COUNT(*) AS $COUNT_COL
         |    FROM ${table.name}
         |    GROUP BY $columnSelection
        """.stripMargin

    val statement = connection.createStatement()
    statement.execute(query)

    Some(FrequenciesAndNumRowsWithJdbc(frequenciesTable))
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): HistogramMetric = {

    state match {

      case Some(theState) =>

        val value: Try[Distribution] = Try {

        val (histogramDetails, binCount) = theState match {

          case sparkState: FrequenciesAndNumRowsWithSpark =>
            val topNRows = sparkState.frequencies.rdd.top(maxDetailBins)(OrderByAbsoluteCount)
            val binCount = sparkState.frequencies.count()

            val histogramDetails = topNRows
              .map { case Row(discreteValue: String, absolute: Long) =>
                val ratio = absolute.toDouble / sparkState.numRows
                discreteValue -> DistributionValue(absolute, ratio)
              }
              .toMap

            (histogramDetails, binCount)

          case jdbcState: FrequenciesAndNumRowsWithJdbc =>
            val topNFreq = topNFrequencies(jdbcState.table)
            val binCount = jdbcState.numRows()

            val histogramDetails = topNFreq.mapValues(absolute => {
              val ratio = absolute.toDouble / theState.numRows
              DistributionValue(absolute, ratio)
            })

            (histogramDetails, binCount)
        }

        Distribution(histogramDetails, binCount)
      }

      HistogramMetric(column, value)

      case None =>
        HistogramMetric(column, Failure(Analyzers.emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditionsWithSpark: Seq[StructType => Unit] = {
    PARAM_CHECK :: Preconditions.hasColumn(column) :: Nil
  }

  override def preconditionsWithJdbc: Seq[Table => Unit] = {
    PARAM_CHECK :: JdbcPreconditions.hasTable() :: JdbcPreconditions.hasColumn(column) :: Nil
  }

  def topNFrequencies(table: Table): Map[String, Long] = {
    val query =
      s"""
         |SELECT
         | $column, $COUNT_COL
         |FROM
         | ${table.name}
         |ORDER BY $COUNT_COL DESC
         |FETCH FIRST $maxDetailBins ROWS ONLY
      """.stripMargin

    var frequencies = Map[String, Long]()

    val statement = table.jdbcConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    while (result.next()) {
      val col = Option(result.getObject(1)) match {
        case Some(notNullVal) => notNullVal.toString
        case None => Histogram.NullFieldReplacement
      }
      val frequency = result.getLong(2)
      frequencies += (col -> frequency)
    }

    frequencies
  }
}

object Histogram {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
}

object OrderByAbsoluteCount extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    x.getLong(1).compareTo(y.getLong(1))
  }
}
