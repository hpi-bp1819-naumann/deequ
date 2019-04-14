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

import java.io._
import java.util.UUID.randomUUID

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.repository._
import com.amazon.deequ.serialization.json.StatisticsResultSerde
import com.amazon.deequ.statistics.Statistic
import org.apache.commons.io.IOUtils


/** A Repository implementation using a file system */
class DiskMetricsRepository(path: String) extends MetricsRepository {

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    * @param analyzerContext The resulting AnalyzerContext of an Analysis
    */
  override def save(resultKey: ResultKey, analyzerContext: ComputedStatistics): Unit = {

    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = ComputedStatistics(successfulMetrics)

    val previousAnalysisResults = load().get().filter(_.resultKey != resultKey)

    val serializedResult = StatisticsResultSerde.serialize(
      previousAnalysisResults ++ Seq(StatisticsResult(resultKey, analyzerContextWithSuccessfulValues))
    )

    DiskMetricsRepository.writeToFileOnDisk(path) {
      val bytes = serializedResult.getBytes(DiskMetricsRepository.CHARSET_NAME)
      _.write(bytes)
    }
  }

  /**
    * Get a AnalyzerContext saved using exactly the same resultKey if present
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    */
  override def loadByKey(resultKey: ResultKey): Option[ComputedStatistics] = {
    load().get().find(_.resultKey == resultKey).map(_.computedStatistics)
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): MetricsRepositoryMultipleResultsLoader = {
    new FileSystemMetricsRepositoryMultipleResultsLoader(path)
  }
}

class FileSystemMetricsRepositoryMultipleResultsLoader(path: String)
  extends MetricsRepositoryMultipleResultsLoader {

  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[Statistic]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
    * Filter out results that don't have specific values for specific tags
    *
    * @param tagValues Map with tag names and the corresponding values to filter for
    */
  def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
    * Choose all metrics that you want to load
    *
    * @param analyzers A sequence of analyers who's resulting metrics you want to load
    */
  def forStatistics(analyzers: Seq[Statistic])
    : MetricsRepositoryMultipleResultsLoader = {

    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a smaller value
    *
    * @param dateTime The maximum dateTime of AnalysisResults to look at
    */
  def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a greater value
    *
    * @param dateTime The minimum dateTime of AnalysisResults to look at
    */
  def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /** Get the AnalysisResult */
  def get(): Seq[StatisticsResult] = {

    val allResults = DiskMetricsRepository
      .readFromFileOnDisk(path) {
        IOUtils.toString(_, DiskMetricsRepository.CHARSET_NAME)
      }
      .map { fileContent => StatisticsResultSerde.deserialize(fileContent) }
      .getOrElse(Seq.empty)

    val selection = allResults
      .filter { result => after.isEmpty || after.get <= result.resultKey.dataSetDate }
      .filter { result => before.isEmpty || result.resultKey.dataSetDate <= before.get }
      .filter { result => tagValues.isEmpty ||
        tagValues.get.toSet.subsetOf(result.resultKey.tags.toSet) }

    selection
      .map { analysisResult =>

        val requestedMetrics = analysisResult
          .computedStatistics
          .metricMap
          .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.get.contains(analyzer))

        val requestedAnalyzerContext = ComputedStatistics(requestedMetrics)

        StatisticsResult(analysisResult.resultKey, requestedAnalyzerContext)
      }
  }
}


object DiskMetricsRepository {

  val CHARSET_NAME = "UTF-8"

  def apply(path: String): DiskMetricsRepository = {
    new DiskMetricsRepository(path)
  }

  private[jdbc] def writeToFileOnDisk(path: String, overwrite: Boolean = false)
                       (writeFunc: DataOutputStream => Unit): Unit = {

    val uuid = randomUUID().toString
    LocalDiskUtils.writeToFileOnDisk(s"$path/$uuid.json", overwrite)(writeFunc)
  }

  private[jdbc] def readFromFileOnDisk[T](path: String)
                           (readFunc: DataInputStream => T): Option[T] = {

    Some(LocalDiskUtils.readFromFileOnDisk[T](path)(readFunc))
  }
}