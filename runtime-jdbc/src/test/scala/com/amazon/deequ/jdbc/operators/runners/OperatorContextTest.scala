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

package com.amazon.deequ.jdbc.operators.runners

import java.sql.Connection

import com.amazon.deequ.JdbcContextSpec
import com.amazon.deequ.jdbc.OperatorList
import com.amazon.deequ.runtime.jdbc.executor.OperatorResults
import com.amazon.deequ.runtime.jdbc.operators.{CompletenessOp, DistinctnessOp, SizeOp, UniquenessOp}
import com.amazon.deequ.serialization.json.SimpleResultSerde
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}


class OperatorContextTest extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  "AnalyzerContext" should {

    "only include specific metrics in returned DataFrame if requested" in
      withJdbc { connection =>

        evaluate(connection) { results =>

          val metricsForAnalyzers = Seq(CompletenessOp("att1"), UniquenessOp(Seq("att1", "att2")))

          val successMetricsAsDataFrame = OperatorResults
              .successMetricsAsJson(results, metricsForAnalyzers)

          val metricsList = Seq(
            Seq("Column", "att1", "Completeness", 1.0),
            Seq("Mutlicolumn", "att1,att2", "Uniqueness", 0.25))

          val expected = serializeMetricsList(metricsList)

          assert(successMetricsAsDataFrame == expected)
        }
    }

    "correctly return Json that is formatted as expected" in
      withJdbc { connection =>

        evaluate(connection) { results =>

          val successMetricsResultsJson = OperatorResults.successMetricsAsJson(results)

          val expectedJson =
            """[{"entity":"Dataset","instance":"*","name":"Size","value":4.0},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(successMetricsResultsJson, expectedJson)
        }
      }

    "only include requested metrics in returned Json" in
      withJdbc { connection =>

        evaluate(connection) { results =>

          val metricsForAnalyzers = Seq(CompletenessOp("att1"), UniquenessOp(Seq("att1", "att2")))

          val successMetricsResultsJson =
            OperatorResults.successMetricsAsJson(results, metricsForAnalyzers)

          val expectedJson =
            """[{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25}]"""
            .stripMargin.replaceAll("\n", "")

          assertSameJson(successMetricsResultsJson, expectedJson)
        }
      }
  }

  private[this] def evaluate(connection: Connection)(test: OperatorResults => Unit): Unit = {

    val data = getTableFull(connection)

    val results = createAnalysis().run(data)

    test(results)
  }

  private[this] def createAnalysis(): OperatorList = {
    OperatorList()
      .addAnalyzer(SizeOp())
      .addAnalyzer(DistinctnessOp("item"))
      .addAnalyzer(CompletenessOp("att1"))
      .addAnalyzer(UniquenessOp(Seq("att1", "att2")))
  }

  private[this] def serializeMetricsList(metricsList: Seq[Seq[Any]]): String = {

    SimpleResultSerde.serialize(metricsList.map { expectedOutputs =>
      Map(
        "entity" -> expectedOutputs.head,
        "instance" -> expectedOutputs(1),
        "name" -> expectedOutputs(2),
        "value" -> expectedOutputs(3)
      )
    })
  }

  private[this] def assertSameJson(jsonA: String, jsonB: String): Unit = {
    assert(SimpleResultSerde.deserialize(jsonA) ==
      SimpleResultSerde.deserialize(jsonB))
  }
}
