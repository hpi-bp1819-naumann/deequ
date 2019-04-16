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

package com.amazon.deequ.profiles

import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.{InMemoryMetricsRepository, ResultKey}
import com.amazon.deequ.runtime.jdbc.{JdbcDataset, JdbcEngine}
import com.amazon.deequ.serialization.json.JsonSerializer
import com.amazon.deequ.statistics.{Completeness, Size}
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.{ComputedStatistics, JdbcContextSpec, VerificationSuite}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class RDDColumnProfilerRunnerTest extends WordSpec with Matchers with JdbcContextSpec
  with FixtureSupport {

  "Column Profiler runner" should {

    "save results if specified so they can be reused by other runners" in
      withJdbc { connection =>

        val engine = JdbcEngine(connection)
        val df = JdbcDataset(getTableWithNumericValues(connection))

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        val analyzerContext = engine.compute(df, analyzers)

        assert(analyzerContext.metricMap.size == 2)
        assert(analyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
      }

    "only append results to repository without unnecessarily overwriting existing ones" in
      withJdbc { connection =>

        val df = JdbcDataset(getTableWithNumericValues(connection))

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val completeMetricResults = VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzers(analyzers).saveOrAppendResult(resultKey).run().metrics

        val completeAnalyzerContext = ComputedStatistics(completeMetricResults)

        // Calculate and save results for first analyzer
        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        // Calculate and append results for second analyzer
        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        assert(completeAnalyzerContext.metricMap.size == 2)
        assert(completeAnalyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withJdbc { connection =>

      val engine = JdbcEngine(connection)
      val df = JdbcDataset(getTableWithNumericValues(connection))

      val repository = new InMemoryMetricsRepository
      val resultKey = ResultKey(0, Map.empty)

      val analyzers = Size() :: Completeness("item") :: Nil

      val expectedAnalyzerContextOnLoadByKey = engine.compute(df, analyzers)

      val resultWhichShouldBeOverwritten = ComputedStatistics(Map(Size() -> DoubleMetric(
        Entity.Dataset, "", "", Try(100.0))))

      repository.save(resultKey, resultWhichShouldBeOverwritten)

      // This should overwrite the previous Size value
      ColumnProfilerRunner().onData(df)
        .useRepository(repository)
        .saveOrAppendResult(resultKey).run()

      assert(expectedAnalyzerContextOnLoadByKey.metricMap.size == 2)
      assert(expectedAnalyzerContextOnLoadByKey.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
    }

//    "should write output files to specified locations" in withJdbc { connection =>
//
//      val engine = JdbcEngine(connection)
//      val data = JdbcDataset(getTableWithNumericValues(connection))
//
//      val tempDir = TempFileUtils.tempDir("constraintSuggestionOuput")
//      val columnProfilesPath = tempDir + "/column-profiles.json"
//
//      ColumnProfilerRunner().onData(data)
//        .saveColumnProfilesJsonToPath(columnProfilesPath)
//        .run()
//
//      LocalDiskUtils.readFromFileOnDisk(columnProfilesPath) {
//        inputStream => assert(inputStream.read() > 0)
//      }
//    }
  }

  private[this] def assertConstraintSuggestionResultsEquals(
    expectedResult: ColumnProfiles,
    actualResult: ColumnProfiles): Unit = {

    assert(expectedResult == actualResult)

    val expectedConstraintSuggestionJson = JsonSerializer.columnsProfiles(
      expectedResult.profiles.values.toSeq)

    val actualConstraintSuggestionJson = JsonSerializer.columnsProfiles(
      actualResult.profiles.values.toSeq)

    assert(expectedConstraintSuggestionJson == actualConstraintSuggestionJson)
  }
}
