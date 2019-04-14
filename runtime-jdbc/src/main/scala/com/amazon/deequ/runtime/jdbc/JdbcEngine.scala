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

import java.sql.Connection

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.profiles.ColumnProfiles
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime._
import com.amazon.deequ.runtime.jdbc.executor.{JdbcExecutor, OperatorResults}
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.statistics.{Statistic, _}

case class JdbcEngine(connection: Connection) extends Engine[Connection] {

  override def compute(
                        data: Dataset[Connection],
                        statistics: Seq[Statistic],
                        aggregateWith: Option[StateLoader[Connection]] = None,
                        saveStatesWith: Option[StatePersister[Connection]] = None,
                        engineRepositoryOptions: EngineRepositoryOptions)
  : ComputedStatistics = {

    val analyzers = statistics.map { JdbcEngine.matchingOperator }

    val sparkStateLoader = aggregateWith.map { _.asInstanceOf[JdbcStateLoader] }
    val sparkStatePersister = saveStatesWith.map { _.asInstanceOf[JdbcStatePersister] }

    val analysisResult = JdbcExecutor.doAnalysisRun(
      data.asInstanceOf[JdbcDataset].table,
      analyzers,
      aggregateWith = sparkStateLoader,
      saveStatesWith = sparkStatePersister
    )

    val statisticsAndResults = analysisResult.metricMap
      .map { case (analyzer, metric) =>
        JdbcEngine.matchingStatistic(analyzer) -> metric
      }
      .toMap[Statistic, Metric[_]]

    ComputedStatistics(statisticsAndResults)
  }

  //TODO
  override def splitTrainTestSets(
                                   data: Dataset[Connection],
                                   testsetRatio: Option[Double],
                                   testsetSplitRandomSeed: Option[Long])
  : (Dataset[Connection], Option[Dataset[Connection]]) = {

    val table = data.asInstanceOf[JdbcDataset].table

    /*if (testsetRatio.isDefined) {

      val trainsetRatio = 1.0 - testsetRatio.get
      val Array(trainSplit, testSplit) =
        if (testsetSplitRandomSeed.isDefined) {
          table.randomSplit(Array(trainsetRatio, testsetRatio.get), testsetSplitRandomSeed.get)
        } else {
          table.randomSplit(Array(trainsetRatio, testsetRatio.get))
        }
      (JdbcDataset(trainSplit), Some(JdbcDataset(testSplit)))
    } else {*/
      (data, None)
    //}
  }

  override def profile(
                        dataset: Dataset[Connection],
                        restrictToColumns: Option[Seq[String]],
                        lowCardinalityHistogramThreshold: Int,
                        printStatusUpdates: Boolean,
                        metricsRepository: Option[MetricsRepository],
                        reuseExistingResultsUsingKey: Option[ResultKey],
                        failIfResultsForReusingMissing: Boolean,
                        saveInMetricsRepositoryUsingKey: Option[ResultKey])
  : ColumnProfiles = {

    val table = dataset.asInstanceOf[JdbcDataset].table

    RDDColumnProfiler.profile(
      dataset.asInstanceOf[JdbcDataset].table,
      restrictToColumns,
      printStatusUpdates,
      lowCardinalityHistogramThreshold,
      metricsRepository,
      reuseExistingResultsUsingKey,
      failIfResultsForReusingMissing,
      saveInMetricsRepositoryUsingKey
    )
  }
}

object JdbcEngine {

  private[deequ] def matchingOperator(statistic: Statistic): Operator[_, Metric[_]] = {

    statistic match {

      case size: Size => SizeOp(size.where)

      case completeness: Completeness => CompletenessOp(completeness.column, completeness.where)

      case compliance: Compliance => ComplianceOp(compliance.instance, compliance.predicate, compliance.where)

      case patternMatch: PatternMatch => PatternMatchOp(patternMatch.column, patternMatch.pattern, patternMatch.where)

      case sum: Sum => SumOp(sum.column, sum.where)

      case mean: Mean => MeanOp(mean.column, mean.where)

      case minimum: Minimum => MinimumOp(minimum.column, minimum.where)

      case maximum: Maximum => MaximumOp(maximum.column, maximum.where)

      case histogram : Histogram => HistogramOp(histogram.column, maxDetailBins = histogram.maxDetailBins)

      case uniqueness: Uniqueness => UniquenessOp(uniqueness.columns)

      case distinctness: Distinctness => DistinctnessOp(distinctness.columns)

      case uniqueValueRatio: UniqueValueRatio => UniqueValueRatioOp(uniqueValueRatio.columns)

      case entropy: Entropy => EntropyOp(entropy.column)

      //TODO: case mutualInformation: MutualInformation => MutualInformationOp(mutualInformation.columns)

      case dataType: DataType => DataTypeOp(dataType.column, dataType.where)

      /*case approxCountDistinct: ApproxCountDistinct =>
        ApproxCountDistinctOp(approxCountDistinct.column, approxCountDistinct.where)*/

      case correlation: Correlation =>
        CorrelationOp(correlation.firstColumn, correlation.secondColumn, correlation.where)

      case stdDev: StandardDeviation => StandardDeviationOp(stdDev.column, stdDev.where)

      /*case approxQuantile: ApproxQuantile => ApproxQuantileOp(approxQuantile.column, approxQuantile.quantile)

      case approxQuantiles: ApproxQuantiles => ApproxQuantilesOp(approxQuantiles.column, approxQuantiles.quantiles)*/

      case countDistinct: CountDistinct => CountDistinctOp(countDistinct.columns)

      case _ => throw new IllegalArgumentException(s"Unable to handle statistic $statistic.")
    }
  }

  private[deequ] def matchingStatistic(analyzer: Operator[_, Metric[_]]): Statistic = {

    analyzer match {

      case size: SizeOp => Size(size.where)

      case completeness: CompletenessOp => Completeness(completeness.column, completeness.where)

      case compliance: ComplianceOp => Compliance(compliance.instance, compliance.predicate, compliance.where)

      case patternMatch: PatternMatchOp => PatternMatch(patternMatch.column, patternMatch.pattern, patternMatch.where)

      case sum: SumOp => Sum(sum.column, sum.where)

      case mean: MeanOp => Mean(mean.column, mean.where)

      case minimum: MinimumOp => Minimum(minimum.column, minimum.where)

      case maximum: MaximumOp => Maximum(maximum.column, maximum.where)

      case histogram: HistogramOp => Histogram(histogram.column, maxDetailBins = histogram.maxDetailBins)

      case uniqueness: UniquenessOp => Uniqueness(uniqueness.columns)

      case distinctness: DistinctnessOp => Distinctness(distinctness.columns)

      case uniqueValueRatio: UniqueValueRatioOp => UniqueValueRatio(uniqueValueRatio.columns)

      case entropy: EntropyOp => Entropy(entropy.column)

      //case mutualInformation: MutualInformationOp => MutualInformation(mutualInformation.columns)

      case dataType: DataTypeOp => DataType(dataType.column, dataType.where)

      /*case approxCountDistinct: ApproxCountDistinctOp =>
        ApproxCountDistinct(approxCountDistinct.column, approxCountDistinct.where)*/

      case correlation: CorrelationOp =>
        Correlation(correlation.firstColumn, correlation.secondColumn, correlation.where)

      case stdDev: StandardDeviationOp =>
        StandardDeviation(stdDev.column, stdDev.where)

      /*case approxQuantile: ApproxQuantileOp =>
        ApproxQuantile(approxQuantile.column, approxQuantile.quantile)

      case approxQuantiles: ApproxQuantilesOp =>
        ApproxQuantiles(approxQuantiles.column, approxQuantiles.quantiles)*/

      case countDistinctOp: CountDistinctOp => CountDistinct(countDistinctOp.columns)

      case _ =>
        throw new IllegalArgumentException(s"Unable to handle operator $analyzer.")
    }
  }

  private[deequ] def analyzerContextToComputedStatistics(analyzerContext: OperatorResults): ComputedStatistics = {
    val metrics = analyzerContext.metricMap.map { case (analyzer, metric) =>
      matchingStatistic(analyzer) -> metric
    }
      .toMap[Statistic, Metric[_]]

    ComputedStatistics(metrics)
  }

  private[deequ] def computedStatisticsToAnalyzerContext(computedStatistics: ComputedStatistics): OperatorResults = {
    OperatorResults(computedStatistics.metricMap.map { case (analyzer: Statistic, metric: Metric[_]) =>
      matchingOperator(analyzer).asInstanceOf[Operator[State[_], Metric[_]]] -> metric
    }
      .toMap[Operator[_, Metric[_]], Metric[_]])

    //AnalyzerContext(metrics)
  }

  private[deequ] def computeOn(data: Table, statistics: Seq[Statistic]): ComputedStatistics = {
    val engine = JdbcEngine(data.jdbcConnection)
    engine.compute(JdbcDataset(data), statistics)
  }


}