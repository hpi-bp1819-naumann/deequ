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

package com.amazon.deequ.repository

import java.sql.Connection
import java.time.{LocalDate, ZoneOffset}

import com.amazon.deequ._
import com.amazon.deequ.anomalydetection.{OnlineNormalStrategy, RateOfChangeStrategy}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{DiskMetricsRepository, JdbcDataset}
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable
import scala.util.Success

class MetricsRepositoryAnomalyDetectionIntegrationTest extends WordSpec with Matchers
  with JdbcContextSpec with FixtureSupport {

  "Anomaly Detection" should {

    "work using the InMemoryMetricsRepository" in withJdbc { connection =>

      val repository = new InMemoryMetricsRepository()

      testAnomalyDetection(connection, repository)

    }

    "work using the FileSystemMetricsRepository" in withJdbc { connection =>

      val tempDir = TempFileUtils.tempDir("fileSystemRepositoryTest")
      val repository = DiskMetricsRepository(tempDir + "repository-test.json")

      testAnomalyDetection(connection, repository)
    }
  }

  private[this] def testAnomalyDetection(
    connection: Connection,
    repository: MetricsRepository)
  : Unit = {

    val data = getTestData(connection)

    // Fill repository with some fake results from previous runs for July 2018
    fillRepositoryWithPreviousResults(repository)

    // Some other checks and analyzers we are interested in not related to the anomaly detection
    val (otherCheck, additionalRequiredAnalyzers) = getNormalCheckAndRequiredAnalyzers

    // This method is where the interesting stuff happens
    val verificationResult = createAnomalyChecksAndRunEverything(data, repository, otherCheck,
      additionalRequiredAnalyzers)

    printConstraintResults(verificationResult)

    assertAnomalyCheckResultsAreCorrect(verificationResult)
  }

  def getTestData(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("origin", StringType) ::
        JdbcStructField("sales", IntegerType) ::
        JdbcStructField("marketplace", StringType) :: Nil)

    val rowData = Seq(
      Seq("item1", "US", 100, "EU"),
      Seq("item1", "US", 1000, "EU"),
      Seq("item1", "US", 20, "EU"),

      Seq("item2", "DE", 20, "EU"),
      Seq("item2", "DE", 333, "EU"),

      Seq("item3", null, 12, "EU"),
      Seq("item4", null, 45, "EU"),
      Seq("item5", null, 123, "EU"))

    fillTableWithData("testData", schema, rowData, connection)
  }

  private[this] def fillRepositoryWithPreviousResults(repository: MetricsRepository): Unit = {

     (1 to 30).foreach { pastDay =>

      val pastResultsEU = Map(
        Size() -> DoubleMetric(Entity.Dataset, "*", "Size", Success(math.floor(pastDay / 3))),
        Mean("sales") -> DoubleMetric(Entity.Column, "sales", "Mean", Success(pastDay * 7))
      ).asInstanceOf[Map[Statistic, Metric[_]]]

      val pastResultsNA = Map(
        Size() -> DoubleMetric(Entity.Dataset, "*", "Size", Success(pastDay)),
        Mean("sales") -> DoubleMetric(Entity.Column, "sales", "Mean", Success(pastDay * 9))
      ).asInstanceOf[Map[Statistic, Metric[_]]]

      val analyzerContextEU = ComputedStatistics(pastResultsEU)
      val analyzerContextNA = ComputedStatistics(pastResultsNA)

      val dateTime = createDate(2018, 7, pastDay)

      repository.save(ResultKey(dateTime, Map("marketplace" -> "EU")), analyzerContextEU)
      repository.save(ResultKey(dateTime, Map("marketplace" -> "NA")), analyzerContextNA)
    }
  }

  private[this] def getNormalCheckAndRequiredAnalyzers: (Check, Seq[Statistic]) = {

    val check = Check(CheckLevel.Error, "check")
      .isComplete("item")
      .isComplete("origin")
      .isContainedIn("marketplace", Array("EU"))
      .isNonNegative("sales")

    val requiredAnalyzers = Seq(Maximum("sales"), Minimum("sales"))

    (check, requiredAnalyzers)
  }

  private[this] def createAnomalyChecksAndRunEverything(
    data: Table,
    repository: MetricsRepository,
    otherCheck: Check,
    additionalRequiredAnalyzers: Seq[Statistic])
  : VerificationResult = {

    // We only want to use historic data with the EU tag for the anomaly checks since the new
    // data point is from the EU marketplace
    val filterEU = Map("marketplace" -> "EU")

    // We only want to use data points before the date time associated with the current
    // data point and only ones that are from 2018
    val afterDateTime = createDate(2018, 1, 1)
    val beforeDateTime = createDate(2018, 8, 1)

    // Config for the size anomaly check
    val sizeAnomalyCheckConfig = AnomalyCheckConfig(CheckLevel.Error, "Size only increases",
      filterEU, Some(afterDateTime), Some(beforeDateTime))
    val sizeAnomalyDetectionStrategy = RateOfChangeStrategy(Some(0))

    // Config for the mean sales anomaly check
    val meanSalesAnomalyCheckConfig = AnomalyCheckConfig(
      CheckLevel.Warning,
      "Sales mean within 2 standard deviations",
      filterEU,
      Some(afterDateTime),
      Some(beforeDateTime)
    )
    val meanSalesAnomalyDetectionStrategy = OnlineNormalStrategy(upperDeviationFactor = Some(2),
      ignoreAnomalies = false)

    // ResultKey to be used when saving the results of this run
    val currentRunResultKey = ResultKey(createDate(2018, 8, 1), Map("marketplace" -> "EU"))

    val dataset = JdbcDataset(data)

    VerificationSuite()
      .onData(dataset)
      .addCheck(otherCheck)
      .addRequiredAnalyzers(additionalRequiredAnalyzers)
      .useRepository(repository)
      // Add the Size anomaly check
      .addAnomalyCheck(sizeAnomalyDetectionStrategy, Size(), Some(sizeAnomalyCheckConfig))
      // Add the Mean sales anomaly check
      .addAnomalyCheck(meanSalesAnomalyDetectionStrategy, Mean("sales"),
        Some(meanSalesAnomalyCheckConfig))
      // Save new data point in the repository after we calculated everything
      .saveOrAppendResult(currentRunResultKey)
      .run()
  }

  private[this] def assertAnomalyCheckResultsAreCorrect(
    verificationResult: VerificationResult)
  : Unit = {

    // New size value is 8, that is an anomaly because it is lower than the last value, 10
    val sizeAnomalyCheckWithResult = verificationResult.checkResults
      .filterKeys(_.description == "Size only increases")
      .head
    val (_, checkResultSizeAnomalyCheck) = sizeAnomalyCheckWithResult

    assert(CheckStatus.Error == checkResultSizeAnomalyCheck.status)

    // New Mean sales value is 206.625, that is not an anomaly because the previous values are
    // (1 to 30) * 7 and it is within the range of 2 standard deviations
    // (mean: ~111, stdDeviation: ~62)
    val meanSalesAnomalyCheckWithResult = verificationResult.checkResults
      .filterKeys(_.description == "Sales mean within 2 standard deviations")
      .head
    val (_, checkResultMeanSalesAnomalyCheck) = meanSalesAnomalyCheckWithResult

    assert(CheckStatus.Success == checkResultMeanSalesAnomalyCheck.status)
  }

  private[this] def printConstraintResults(result: VerificationResult): Unit = {

    println(s"\n\n### CONSTRAINT RESULTS ###")
    println("\n\t--- Successful constraints ---")
    result.checkResults.foreach { case (_, checkResult) =>

      checkResult.constraintResults
        .filter { _.status == ConstraintStatus.Success }
        .foreach { constraintResult =>
          println(s"\t${constraintResult.constraint}")
        }
    }

    println("\n\t--- Failed constraints ---")
    result.checkResults.foreach { case (_, checkResult) =>

      checkResult.constraintResults
        .filter { _.status != ConstraintStatus.Success }
        .foreach { constraintResult =>
          println(s"\t${constraintResult.constraint}: ${constraintResult.message.get}")
        }
    }
  }

  private[this] def createDate(year: Int, month: Int, day: Int): Long = {
    LocalDate.of(year, month, day).atTime(0, 0, 0).toEpochSecond(ZoneOffset.UTC)
  }

}
