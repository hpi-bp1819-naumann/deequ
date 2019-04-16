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

import com.amazon.deequ.runtime.jdbc.JdbcEngine
import com.amazon.deequ.serialization.json.SimpleResultSerde
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.{ComputedStatistics, JdbcContextSpec}
import org.scalatest.{Matchers, WordSpec}

class MetricsRepositoryMultipleResultsLoaderTest extends WordSpec with Matchers
  with JdbcContextSpec with FixtureSupport {

  private[this] val DATE_ONE = createDate(2017, 10, 14)
  private[this] val DATE_TWO = createDate(2017, 10, 15)

  private[this] val REGION_EU = Map("Region" -> "EU")
  private[this] val REGION_NA = Map("Region" -> "NA")
  private[this] val REGION_EU_AND_DATASET_NAME = Map("Region" -> "EU", "dataset_name" -> "Some")
  private[this] val REGION_NA_AND_DATASET_VERSION =
    Map("Region" -> "NA", "dataset_version" -> "2.0")

  "RepositoryMultipleResultsLoader" should {
//
//    "correctly return a DataFrame of multiple AnalysisResults that is formatted as expected" in
//      withSparkSession { session =>
//
//        evaluate(session) { (results, repository) =>
//
//          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//          repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//          val analysisResultsAsDataFrame = repository.load()
//            .getSuccessMetricsAsDataFrame(session)
//
//          import session.implicits._
//          val expected = Seq(
//            // First analysisResult
//            ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
//            ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
//            ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"),
//            // Second analysisResult
//            ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA"),
//            ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"),
//            ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
//            .toDF("entity", "instance", "name", "value", "dataset_date", "region")
//
//          assertSameRows(analysisResultsAsDataFrame, expected)
//        }
//    }
//
    "correctly return Json of multiple AnalysisResults that is formatted as expected" in
      withJdbc { connection =>

        evaluate(connection) { (results, repository) =>

          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
          repository.save(ResultKey(DATE_TWO, REGION_NA), results)

          val analysisResultsAsJson = repository.load()
            .getSuccessMetricsAsJson()

          val expected =
            s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |
              |{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"NA", "dataset_date":$DATE_TWO},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"NA", "dataset_date":$DATE_TWO},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"NA", "dataset_date":$DATE_TWO},
              |{"entity":"Mutlicolumn","instance":"att1,att2","name":"Uniqueness","value":0.25,
              |"region":"NA", "dataset_date":$DATE_TWO}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(analysisResultsAsJson, expected)
        }
      }
//
//    "return empty DataFrame if get returns an empty Sequence of AnalysisResults" in
//      withSparkSession { session =>
//
//        evaluate(session) { (results, repository) =>
//
//          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
//          repository.save(ResultKey(DATE_TWO, REGION_NA), results)
//
//          val analysisResultsAsDataFrame = repository.load()
//            .after(DATE_TWO)
//            .before(DATE_ONE)
//            .getSuccessMetricsAsDataFrame(session)
//
//          import session.implicits._
//          val expected = Seq.empty[(String, String, String, Double, Long)]
//            .toDF("entity", "instance", "name", "value", "dataset_date")
//
//          assertSameRows(analysisResultsAsDataFrame, expected)
//        }
//    }
//
    "return empty Json Array if get returns an empty Sequence of AnalysisResults" in
      withJdbc { connection =>

        evaluate(connection) { (results, repository) =>

          repository.save(ResultKey(DATE_ONE, REGION_EU), results)
          repository.save(ResultKey(DATE_TWO, REGION_NA), results)

          val analysisResultsAsJson = repository.load()
              .after(DATE_TWO)
              .before(DATE_ONE)
              .getSuccessMetricsAsJson()

          val expected = """[]"""

          assertSameJson(analysisResultsAsJson, expected)
        }
      }
//
//    "support saving data with different tags and returning DataFrame with them" in
//      withSparkSession { session =>
//
//        evaluate(session) { (results, repository) =>
//
//          repository.save(ResultKey(DATE_ONE, REGION_EU_AND_DATASET_NAME), results)
//          repository.save(ResultKey(DATE_TWO, REGION_NA_AND_DATASET_VERSION), results)
//
//          val analysisResultsAsDataFrame = repository.load()
//            .getSuccessMetricsAsDataFrame(session)
//
//          import session.implicits._
//          val expected = Seq(
//            // First analysisResult
//            ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU",
//              null.asInstanceOf[String], "Some"),
//            ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU",
//              null.asInstanceOf[String], "Some"),
//            ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU",
//              null.asInstanceOf[String], "Some"),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU",
//              null.asInstanceOf[String], "Some"),
//            // Second analysisResult
//            ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA",
//              "2.0", null.asInstanceOf[String]),
//            ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA",
//              "2.0", null.asInstanceOf[String]),
//            ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA",
//              "2.0", null.asInstanceOf[String]),
//            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA",
//              "2.0", null.asInstanceOf[String]))
//            .toDF("entity", "instance", "name",
//              "value", "dataset_date", "region", "dataset_version", "dataset_name")
//
//          assertSameRows(analysisResultsAsDataFrame, expected)
//        }
//    }
//
    "support saving data with different tags and returning Json with them" in
      withJdbc { connection =>

        evaluate(connection) { (results, repository) =>

          repository.save(ResultKey(DATE_ONE, REGION_EU_AND_DATASET_NAME), results)
          repository.save(ResultKey(DATE_TWO, REGION_NA_AND_DATASET_VERSION), results)

          val analysisResultsAsJson = repository.load()
            .getSuccessMetricsAsJson()

          val expected =
            s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"EU", "dataset_date":$DATE_ONE,
              |"dataset_name":"Some", "dataset_version":null},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE,
              |"dataset_name":"Some", "dataset_version":null},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE,
              |"dataset_name":"Some", "dataset_version":null},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"EU", "dataset_date":$DATE_ONE,
              |"dataset_name":"Some", "dataset_version":null},
              |
              |{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"NA", "dataset_date":$DATE_TWO,
              |"dataset_name":null, "dataset_version":"2.0"},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"NA", "dataset_date":$DATE_TWO,
              |"dataset_name":null, "dataset_version":"2.0"},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"NA", "dataset_date":$DATE_TWO,
              |"dataset_name":null, "dataset_version":"2.0"},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"NA", "dataset_date":$DATE_TWO,
              |"dataset_name":null, "dataset_version":"2.0"}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(analysisResultsAsJson, expected)

          assertSameJson(analysisResultsAsJson, expected)
        }
      }
  }

  private[this] def evaluate(connection: Connection)
      (test: (ComputedStatistics, MetricsRepository) => Unit)
    : Unit = {

    val data = getTableFull(connection)
    val results = JdbcEngine.computeOn(data, createStatistics())
    val repository = createRepository()

    test(results, repository)
  }

  private[this] def createStatistics(): Seq[Statistic] = {
    Size() ::
      Distinctness(Seq("item")) ::
      Completeness("att1") ::
      Uniqueness(Seq("att1", "att2")) :: Nil
  }

  private[this] def createRepository(): MetricsRepository = {
    new InMemoryMetricsRepository()
  }

  private[this] def createDate(year: Int, month: Int, day: Int): Long = {
    LocalDate.of(year, month, day).atTime(10, 10, 10).toEpochSecond(ZoneOffset.UTC)
  }

//  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
//    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
//  }

  private[this] def assertSameJson(jsonA: String, jsonB: String): Unit = {
    assert(SimpleResultSerde.deserialize(jsonA) ==
      SimpleResultSerde.deserialize(jsonB))
  }
}
