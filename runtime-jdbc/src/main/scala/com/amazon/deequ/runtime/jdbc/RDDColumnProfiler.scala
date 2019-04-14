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

package com.amazon.deequ.runtime.jdbc

import com.amazon.deequ.{ComputedStatistics, RepositoryOptions}
import com.amazon.deequ.statistics.DataTypeInstances._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.metrics._
import com.amazon.deequ.profiles.{ColumnProfiles, NumericColumnProfile, StandardColumnProfile}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.jdbc.executor.{JdbcExecutor, OperatorResults, ReusingNotPossibleResultsMissingException}
import com.amazon.deequ.runtime.jdbc.operators.{CompletenessOp, HistogramOp, SizeOp}
import com.amazon.deequ.statistics.{DataTypeInstances, Histogram, Statistic}

import scala.collection.mutable
import scala.util.Success

private[deequ] case class GenericColumnStatistics(
    numRecords: Long,
    inferredTypes: Map[String, DataTypeInstances.Value],
    knownTypes: Map[String, DataTypeInstances.Value],
    typeDetectionHistograms: Map[String, Map[String, Long]],
    approximateNumDistincts: Map[String, Long],
    completenesses: Map[String, Double]) {

  def typeOf(column: String): DataTypeInstances.Value = {
    val inferredAndKnown = inferredTypes ++ knownTypes
    inferredAndKnown(column)
  }
}

private[deequ] case class NumericColumnStatistics(
    means: Map[String, Double],
    stdDevs: Map[String, Double],
    minima: Map[String, Double],
    maxima: Map[String, Double],
    sums: Map[String, Double],
    approxPercentiles: Map[String, Seq[Double]]
)

private[deequ] case class CategoricalColumnStatistics(histograms: Map[String, Distribution])


/** Computes single-column profiles in three scans over the data, intented for large (TB) datasets
  *
  * In the first phase, we compute the number of records, as well as the datatype, approx. num
  * distinct values and the completeness of each column in the sample.
  *
  * In the second phase, we compute min, max and mean for numeric columns (in the future, we should
  * add quantiles once they become scan-shareable)
  *
  * In the third phase, we compute histograms for all columns with less than
  * `lowCardinalityHistogramThreshold` (approx.) distinct values
  *
  */
object RDDColumnProfiler {


  /**
    * Profile a (potentially very large) dataset
    *
    * @param data dataset as dataframe
    * @param restrictToColumns  can contain a subset of columns to profile, otherwise
    *                           all columns will be considered
    * @param lowCardinalityHistogramThreshold the maximum  (estimated) number of distinct values
    *                                         in a column until which we should compute exact
    *                                         histograms for it (defaults to 120)
    * @return
    */
  private[deequ] def profile(
      data: Table,
      restrictToColumns: Option[Seq[String]] = None,
      printStatusUpdates: Boolean = false,
      lowCardinalityHistogramThreshold: Int = ColumnProfiles.DEFAULT_CARDINALITY_THRESHOLD,
      metricsRepository: Option[MetricsRepository] = None,
      reuseExistingResultsUsingKey: Option[ResultKey] = None,
      failIfResultsForReusingMissing: Boolean = false,
      saveInMetricsRepositoryUsingKey: Option[ResultKey] = None)
    : ColumnProfiles = {

    // Ensure that all desired columns exist
    restrictToColumns.foreach { restrictToColumns =>
      restrictToColumns.foreach { columnName =>
        require(data.columns().keySet.contains(columnName), s"Unable to find column $columnName")
      }
    }

    // Find columns we want to profile
    val relevantColumns = getRelevantColumns(data.columns(), restrictToColumns)

    // First pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing generic column statistics in pass (1/3)...")
    }

    val repositoryOptions = RepositoryOptions(
      metricsRepository,
      reuseExistingResultsUsingKey,
      failIfResultsForReusingMissing,
      saveInMetricsRepositoryUsingKey
    )

    // We compute completeness, approximate number of distinct values
    // and type detection for string columns in the first pass
    val analyzersForGenericStats = getAnalyzersForGenericStats(data.columns(), relevantColumns)

    val firstPassResults = JdbcExecutor.doAnalysisRun(
      data,
      analyzersForGenericStats ++ Seq(SizeOp()),
      metricsRepositoryOptions = repositoryOptions
    )

    val genericStatistics = extractGenericStatistics(relevantColumns, data.columns(), firstPassResults)

    // Second pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing numeric column statistics in pass (2/3)...")
    }

    // We cast all string columns that were detected as numeric
    val castedDataForSecondPass = castNumericStringColumns(relevantColumns, data,
      genericStatistics)

    // We compute mean, stddev, min, max for all numeric columns
    val analyzersForSecondPass = getAnalyzersForSecondPass(relevantColumns, genericStatistics)

    val secondPassResults = JdbcExecutor.doAnalysisRun(
      castedDataForSecondPass,
      analyzersForSecondPass,
      metricsRepositoryOptions = repositoryOptions
    )

    val numericStatistics = extractNumericStatistics(secondPassResults)

    // Third pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing histograms of low-cardinality columns in pass (3/3)...")
    }

    // We compute exact histograms for all low-cardinality string columns, find those here
    val targetColumnsForHistograms = findTargetColumnsForHistograms(data.columns(), genericStatistics,
      lowCardinalityHistogramThreshold)

    // Find out, if we have values for those we can reuse
    val analyzerContextExistingValues = getAnalyzerContextWithHistogramResultsForReusingIfNecessary(
      metricsRepository,
      reuseExistingResultsUsingKey,
      targetColumnsForHistograms
    )

    // The columns we need to calculate the histograms for
    val nonExistingHistogramColumns = targetColumnsForHistograms
      .filter { column => analyzerContextExistingValues.metricMap.get(HistogramOp(column)).isEmpty }

    // Calculate and save/append results if necessary
    val histograms: Map[String, Distribution] = getHistogramsForThirdPass(
      data,
      nonExistingHistogramColumns,
      analyzerContextExistingValues,
      printStatusUpdates,
      failIfResultsForReusingMissing,
      metricsRepository,
      saveInMetricsRepositoryUsingKey)

    val thirdPassResults = CategoricalColumnStatistics(histograms)

    createProfiles(relevantColumns, genericStatistics, numericStatistics, thirdPassResults)
  }

  private[this] def getRelevantColumns(
      columns: mutable.LinkedHashMap[String, JdbcDataType],
      restrictToColumns: Option[Seq[String]])
    : Seq[String] = {

    columns.keys
      .filter { field => restrictToColumns.isEmpty || restrictToColumns.get.contains(field) }
      .toSeq
  }

  private[this] def getAnalyzersForGenericStats(
      columns: mutable.LinkedHashMap[String, JdbcDataType],
      relevantColumns: Seq[String])
    : Seq[Operator[_, Metric[_]]] = {

    columns
      .filter { field => relevantColumns.contains(field._1) }
      .flatMap { field =>

        val name = field._1

        if (field._2 == StringType) {
          Seq(CompletenessOp(name), /* ApproxCountDistinctOp(name), */ DataTypeOp(name))
        } else {
          Seq(CompletenessOp(name) /*, ApproxCountDistinctOp(name) */)
        }
      }.toSeq
  }

   private[this] def getAnalyzersForSecondPass(
      relevantColumnNames: Seq[String],
      genericStatistics: GenericColumnStatistics)
    : Seq[Operator[_, Metric[_]]] = {

      relevantColumnNames
        .filter { name => Set(Integral, Fractional).contains(genericStatistics.typeOf(name)) }
        .flatMap { name =>

          val percentiles = (1 to 100).map {
            _.toDouble / 100
          }

          Seq(MinimumOp(name), MaximumOp(name), MeanOp(name), StandardDeviationOp(name),
            SumOp(name) /* , ApproxQuantilesOp(name, percentiles) */)
        }
    }


  private[this] def getAnalyzerContextWithHistogramResultsForReusingIfNecessary(
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsUsingKey: Option[ResultKey],
      targetColumnsForHistograms: Seq[String])
    : OperatorResults = {

    var analyzerContextExistingValues = OperatorResults.empty

    metricsRepository.foreach { metricsRepository =>
      reuseExistingResultsUsingKey.foreach { resultKey =>

        val analyzerContextWithAllPreviousResults = metricsRepository.loadByKey(resultKey)
          .map { JdbcEngine.computedStatisticsToAnalyzerContext }


        analyzerContextWithAllPreviousResults.foreach { analyzerContextWithAllPreviousResults =>

          val relevantEntries = analyzerContextWithAllPreviousResults.metricMap
            .filterKeys {
              case histogram: HistogramOp =>
                targetColumnsForHistograms.contains(histogram.column) &&
                  HistogramOp(histogram.column).equals(histogram)
              case _ => false
            }
          analyzerContextExistingValues = OperatorResults(relevantEntries)
        }
      }
    }

    analyzerContextExistingValues
  }

  private[this] def convertColumnNamesAndDistributionToHistogramWithMetric(
    columnNamesAndDistribution: Map[String, Distribution])
  : Map[Statistic, Metric[_]] = {

    columnNamesAndDistribution
      .map { case (columnName, distribution) =>

        val analyzer = Histogram(columnName)
        val metric = HistogramMetric(columnName, Success(distribution))

        analyzer -> metric
      }
  }

  private[this] def saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext: ComputedStatistics,
      metricsRepository: Option[MetricsRepository],
      saveOrAppendResultsWithKey: Option[ResultKey])
    : Unit = {


    metricsRepository.foreach { repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key).getOrElse(ComputedStatistics.empty)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey ++ resultingAnalyzerContext

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

  /* Cast string columns detected as numeric to their detected type */
  private[jdbc] def castColumn(
      data: Table,
      name: String,
      toType: JdbcDataType)
    : Table = {

    data.withColumn(s"${name}___CASTED", toType, Some(s"CAST($name AS $toType)"))
      .drop(name)
      .withColumnRenamed(s"${name}___CASTED", name)
  }

  private[this] def extractGenericStatistics(
      columns: Seq[String],
      schema: mutable.LinkedHashMap[String, JdbcDataType],
      results: OperatorResults)
    : GenericColumnStatistics = {

    val numRecords = results.metricMap
      .collect { case (_: SizeOp, metric: DoubleMetric) => metric.value.get }
      .head
      .toLong

    val inferredTypes = results.metricMap
      .collect { case (analyzer: DataTypeOp, metric: HistogramMetric) =>
        val typeHistogram = metric.value.get
        analyzer.column -> DataTypeHistogram.determineType(typeHistogram)
      }

    val typeDetectionHistograms = results.metricMap
      .collect { case (analyzer: DataTypeOp, metric: HistogramMetric) =>
        val typeCounts = metric.value.get.values
          .map { case (key, distValue) => key -> distValue.absolute }

        analyzer.column -> typeCounts
      }

    val approximateNumDistincts = Map[String, Long]() /* results.metricMap
      .collect { case (analyzer: ApproxCountDistinctOp, metric: DoubleMetric) =>
        analyzer.column -> metric.value.get.toLong
      } */

    val completenesses = results.metricMap
      .collect { case (analyzer: CompletenessOp, metric: DoubleMetric) =>
        analyzer.column -> metric.value.get
      }

    val knownTypes = schema
      .filter { column => schema.keySet.contains(column._1) }
      .filter { _._2 != StringType }
      .map { field =>
        val knownType = field._2 match {
          case ShortType | LongType | IntegerType => Integral
          case DecimalType | FloatType | DoubleType => Fractional
          case BooleanType => Boolean
          case TimestampType => String  // TODO We should have support for dates in deequ...
          case _ =>
            println(s"Unable to map type ${field._2}")
            Unknown
        }

        field._1 -> knownType
      }
      .toMap

    GenericColumnStatistics(numRecords, inferredTypes, knownTypes, typeDetectionHistograms,
      approximateNumDistincts, completenesses)
  }


  private[this] def castNumericStringColumns(
      columns: Seq[String],
      originalData: Table,
      genericStatistics: GenericColumnStatistics)
    : Table = {

    var castedData = originalData

    columns.foreach { name =>

      castedData = genericStatistics.typeOf(name) match {
        case Integral => castColumn(castedData, name, LongType)
        case Fractional => castColumn(castedData, name, DoubleType)
        case _ => castedData
      }
    }

    castedData
  }


  private[this] def extractNumericStatistics(results: OperatorResults): NumericColumnStatistics = {

    val means = results.metricMap
      .collect { case (analyzer: MeanOp, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val stdDevs = results.metricMap
      .collect { case (analyzer: StandardDeviationOp, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val maxima = results.metricMap
      .collect { case (analyzer: MaximumOp, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val minima = results.metricMap
      .collect { case (analyzer: MinimumOp, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val sums = results.metricMap
      .collect { case (analyzer: SumOp, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val approxPercentiles = Map[String, Seq[Double]]() /* results.metricMap
      .collect {  case (analyzer: ApproxQuantilesOp, metric: KeyedDoubleMetric) =>
        metric.value match {
          case Success(metricValue) =>
            val percentiles = metricValue.values.toSeq.sorted
            Some(analyzer.column -> percentiles)
          case _ => None
        }
      }
      .flatten
      .toMap */

    NumericColumnStatistics(means, stdDevs, minima, maxima, sums, approxPercentiles)
  }

  /* Identifies all columns, which:
   *
   * (1) have type string or boolean
   * (2) have less than `lowCardinalityHistogramThreshold` approximate distinct values
   */
  private[this] def findTargetColumnsForHistograms(
      schema: mutable.LinkedHashMap[String, JdbcDataType],
      genericStatistics: GenericColumnStatistics,
      lowCardinalityHistogramThreshold: Long)
    : Seq[String] = {

    val originalStringOrBooleanColumns = schema
      .flatMap { field =>
        if (field._2 == StringType || field._2 == BooleanType) {
          Some(field._1)
        } else {
          None
        }
      }
      .toSet

    genericStatistics.approximateNumDistincts
      .filter { case (column, _) => originalStringOrBooleanColumns.contains(column) }
      .filter { case (column, _) =>
        genericStatistics.typeOf(column) == String || genericStatistics.typeOf(column) == Boolean
      }
      .filter { case (_, count) => count <= lowCardinalityHistogramThreshold }
      .map { case (column, _) => column }
      .toSeq
  }

  /* Map each the values in the target columns of each row to tuples keyed by column name and value
   * ((column_name, column_value), 1)
   * and count these in a single pass over the data. This is efficient as long as the cardinality
   * of the target columns is low.
   */
  private[this] def computeHistograms(
      data: Table,
      targetColumns: Seq[String])
    : Map[String, Distribution] = {

    val namesToIndexes = data.columns().keys
      .zipWithIndex
      .toMap

    /* val counts = data.rdd
      .flatMap { row =>
        targetColumns.map { column =>

          val index = namesToIndexes(column)
          val valueInColumn = if (row.isNullAt(index)) {
            HistogramOp.NullFieldReplacement
          } else {
            row.get(index).toString
          }

          (column -> valueInColumn, 1)
        }
      }
      .countByKey()

    // Compute the empirical distribution per column from the counts
    targetColumns.map { targetColumn =>

      val countsPerColumn = counts
        .filter { case ((column, _), _) => column == targetColumn }
        .map { case ((_, value), count) => value -> count }
        .toMap

      val sum = countsPerColumn.map { case (_, count) => count }.sum

      val values = countsPerColumn
        .map { case (value, count) => value -> DistributionValue(count, count.toDouble / sum) }

      targetColumn -> Distribution(values, numberOfBins = values.size)
    }
    .toMap */

    throw new Exception("Not yet implemented")
  }

  def getHistogramsForThirdPass(
                                 data: Table,
                                 nonExistingHistogramColumns: Seq[String],
                                 analyzerContextExistingValues: OperatorResults,
                                 printStatusUpdates: Boolean,
                                 failIfResultsForReusingMissing: Boolean,
                                 metricsRepository: Option[MetricsRepository],
                                 saveInMetricsRepositoryUsingKey: Option[ResultKey])
    : Map[String, Distribution] = {

    if (nonExistingHistogramColumns.nonEmpty) {

      // Throw an error if all required metrics should have been calculated before but did not
      if (failIfResultsForReusingMissing) {
        throw new ReusingNotPossibleResultsMissingException(
          "Could not find all necessary results in the MetricsRepository, the calculation of " +
            s"the histograms for these columns would be required: " +
            s"${nonExistingHistogramColumns.mkString(", ")}")
      }

      val columnNamesAndDistribution = computeHistograms(data, nonExistingHistogramColumns)

      // Now merge these results with the results that we want to reuse and store them if specified

      val analyzerAndHistogramMetrics = convertColumnNamesAndDistributionToHistogramWithMetric(
        columnNamesAndDistribution)

      val analyzerContext = ComputedStatistics(analyzerAndHistogramMetrics) ++
        JdbcEngine.analyzerContextToComputedStatistics(analyzerContextExistingValues)

      saveOrAppendResultsIfNecessary(analyzerContext, metricsRepository,
        saveInMetricsRepositoryUsingKey)

      // Return overall results using the more simple Distribution format
      analyzerContext.metricMap
        .map { case (histogram: Histogram, metric: HistogramMetric) if metric.value.isSuccess =>
          histogram.column -> metric.value.get
        }
    } else {
      // We do not need to calculate new histograms
      if (printStatusUpdates) {
        println("### PROFILING: Skipping pass (3/3), no new histograms need to be calculated.")
      }
      analyzerContextExistingValues.metricMap
        .map { case (histogram: HistogramOp, metric: HistogramMetric) if metric.value.isSuccess =>
          histogram.column -> metric.value.get
        }
    }
  }

  private[this] def createProfiles(
      columns: Seq[String],
      genericStats: GenericColumnStatistics,
      numericStats: NumericColumnStatistics,
      categoricalStats: CategoricalColumnStatistics)
    : ColumnProfiles = {

    val profiles = columns
      .map { name =>

        val completeness = genericStats.completenesses(name)
        val approxNumDistinct = genericStats.approximateNumDistincts(name)
        val dataType = genericStats.typeOf(name)
        val isDataTypeInferred = genericStats.inferredTypes.contains(name)
        val histogram = categoricalStats.histograms.get(name)

        val typeCounts = genericStats.typeDetectionHistograms.getOrElse(name, Map.empty)

        val profile = genericStats.typeOf(name) match {

          case Integral | Fractional =>
            NumericColumnProfile(
              name,
              completeness,
              approxNumDistinct,
              dataType,
              isDataTypeInferred,
              typeCounts,
              histogram,
              numericStats.means.get(name),
              numericStats.maxima.get(name),
              numericStats.minima.get(name),
              numericStats.sums.get(name),
              numericStats.stdDevs.get(name),
              numericStats.approxPercentiles.get(name))

          case _ =>
            StandardColumnProfile(
              name,
              completeness,
              approxNumDistinct,
              dataType,
              isDataTypeInferred,
              typeCounts,
              histogram)
        }

        name -> profile
      }
      .toMap

    ColumnProfiles(profiles, genericStats.numRecords)
  }
}