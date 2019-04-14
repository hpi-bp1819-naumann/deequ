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

package com.amazon.deequ.repository.fs

import java.sql.Connection
import java.time.{LocalDate, ZoneOffset}

import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.jdbc.{DiskMetricsRepository, JdbcEngine}
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import com.amazon.deequ.{ComputedStatistics, JdbcContextSpec}
import org.scalatest.WordSpec

import scala.util.{Failure, Success}

class DiskMetricsRepositoryTest extends WordSpec with JdbcContextSpec with FixtureSupport {

  private[this] val DATE_ONE = createDate(2017, 10, 14)
  private[this] val DATE_TWO = createDate(2017, 10, 15)
  private[this] val DATE_THREE = createDate(2017, 10, 16)

  private[this] val REGION_EU = Map("Region" -> "EU")
  private[this] val REGION_NA = Map("Region" -> "NA")

  "File System Repository" should {

//    "save and retrieve AnalyzerContexts" in withJdbc { connection =>
//      evaluate(connection) { (results, repository) =>
//
//        val resultKey = ResultKey(DATE_ONE, REGION_EU)
//        repository.save(resultKey, results)
//
//        val loadedResults = repository.loadByKey(resultKey).get
//
//        val loadedResultsAsDataFrame = successMetricsAsDataFrame(connection, loadedResults)
//        val resultsAsDataFrame = successMetricsAsDataFrame(connection, results)
//
//        assertSameRows(loadedResultsAsDataFrame, resultsAsDataFrame)
//        assert(results == loadedResults)
//      }
//    }

    "save should ignore failed result metrics when saving" in withJdbc { connection =>

      val metrics: Map[Statistic, Metric[_]] = Map(
        Size() -> DoubleMetric(Entity.Column, "Size", "*", Success(5.0)),
        Completeness("ColumnA") ->
          DoubleMetric(Entity.Column, "Completeness", "ColumnA",
            Failure(new RuntimeException("error"))))

      val resultsWithMixedValues = ComputedStatistics(metrics)

      val successMetrics = resultsWithMixedValues.metricMap
        .filter { case (_, metric) => metric.value.isSuccess }

      val resultsWithSuccessfulValues = ComputedStatistics(successMetrics)

      val repository = createRepository()

      val resultKey = ResultKey(DATE_ONE, REGION_EU)

      repository.save(resultKey, resultsWithMixedValues)

      val loadedAnalyzerContext = repository.loadByKey(resultKey).get

      assert(resultsWithSuccessfulValues == loadedAnalyzerContext)
    }

    "saving should work for very long strings as well" in withJdbc { connection =>
      evaluate(connection) { (results, repository) =>

        (1 to 200).foreach(number => repository.save(ResultKey(number, Map.empty), results))

        val loadedAnalyzerContext = repository.loadByKey(ResultKey(200, Map.empty)).get

        assert(results == loadedAnalyzerContext)
      }
    }

//    "save and retrieve AnalysisResults" in withJdbc { connection =>
//
//      evaluate(connection) { (results, repository) =>
//
//        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//        repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//        val analysisResultsAsDataFrame = repository.load()
//          .after(DATE_ONE)
//          .getSuccessMetricsAsDataFrame(connection)
//
//        import connection.implicits._
//        val expected = Seq(
//          // First analysisResult
//          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
//          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
//          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
//          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"),
//          // Second analysisResult
//          ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA"),
//          ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"),
//          ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
//          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
//          .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//        assertSameRows(analysisResultsAsDataFrame, expected)
//      }
//    }
//
//    "only load AnalysisResults within a specific time frame if requested" in
//      withJdbc { sparkSession =>
//
//        evaluate(sparkSession) { (results, repository) =>
//
//          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//          repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//          repository.save(ResultKey(DATE_THREE, REGION_NA), results)
//
//          val analysisResultsAsDataFrame = repository.load()
//            .after(DATE_TWO)
//            .before(DATE_TWO)
//            .getSuccessMetricsAsDataFrame(sparkSession)
//
//          import sparkSession.implicits._
//          val expected = Seq(
//            // Second analysisResult
//            ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA"),
//            ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"),
//            ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
//            .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//          assertSameRows(analysisResultsAsDataFrame, expected)
//        }
//      }
//
//    "only load AnalyzerContexts with specific Tags if requested" in withJdbc { connection =>
//
//      evaluate(connection) { (results, repository) =>
//
//        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//        repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//        val analysisResultsAsDataFrame = repository.load()
//          .after(DATE_ONE)
//          .withTagValues(REGION_EU)
//          .getSuccessMetricsAsDataFrame(connection)
//
//        import connection.implicits._
//        val expected = Seq(
//          // First analysisResult
//          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
//          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
//          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
//          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
//          .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//        assertSameRows(analysisResultsAsDataFrame, expected)
//      }
//    }
//
//    "only include specific metrics in loaded AnalysisResults if requested" in
//      withJdbc { sparkSession =>
//
//        evaluate(sparkSession) { (results, repository) =>
//
//          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//          repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//          val analysisResultsAsDataFrame = repository.load()
//            .after(DATE_ONE)
//            .forAnalyzers(Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2"))))
//            .getSuccessMetricsAsDataFrame(sparkSession)
//
//          import sparkSession.implicits._
//          val expected = Seq(
//            // First analysisResult
//            ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"),
//            // Second analysisResult
//            ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
//            .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//          assertSameRows(analysisResultsAsDataFrame, expected)
//        }
//      }
//
//    "include no metrics in loaded AnalysisResults if requested" in withJdbc { connection =>
//
//      evaluate(connection) { (results, repository) =>
//
//        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//        repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//        val analysisResultsAsDataFrame = repository.load()
//          .after(DATE_ONE)
//          .forAnalyzers(Seq.empty)
//          .getSuccessMetricsAsDataFrame(connection)
//
//        import connection.implicits._
//        val expected = Seq.empty[(String, String, String, Double, Long, String)]
//          .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//        assertSameRows(analysisResultsAsDataFrame, expected)
//      }
//    }

    "return empty Seq if load parameters too restrictive" in withJdbc { connection =>

      evaluate(connection) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResults = repository.load()
          .after(DATE_TWO)
          .before(DATE_ONE)
          .get()

        assert(analysisResults.isEmpty)
      }
    }
  }

  private[this] def evaluate(connection: Connection)
    (test: (ComputedStatistics, MetricsRepository) => Unit): Unit = {

    val data = getTableFull(connection)

    val results = JdbcEngine.computeOn(data, Seq(Size(), Distinctness(Seq("item")), Completeness("att1"),
      Uniqueness(Seq("att1", "att2"))))

    val repository = createRepository()

    test(results, repository)
  }


  private[this] def createDate(year: Int, month: Int, day: Int): Long = {
    LocalDate.of(year, month, day).atTime(10, 10, 10).toEpochSecond(ZoneOffset.UTC)
  }

  private[this] def createRepository(): MetricsRepository = {
    val tempDir = TempFileUtils.tempDir("fileSystemRepositoryTest")
    new DiskMetricsRepository(tempDir)
  }
}
