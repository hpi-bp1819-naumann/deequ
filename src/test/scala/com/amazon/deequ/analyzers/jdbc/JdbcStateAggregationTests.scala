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

package com.amazon.deequ.analyzers.jdbc

import java.sql.Connection

import breeze.numerics.abs
import com.amazon.deequ.analyzers.State
import com.amazon.deequ.metrics.Metric
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

class JdbcStateAggregationTests extends WordSpec with Matchers with JdbcContextSpec
  with JdbcFixtureSupport {

  "State aggregation outside" should {

    "give correct results" in {

      correctlyAggregatesStates(JdbcSize())
      correctlyAggregatesStates(JdbcMaximum("marketplace_id"))
      correctlyAggregatesStates(JdbcMinimum("marketplace_id"))
      correctlyAggregatesStates(JdbcMean("marketplace_id"))
      correctlyAggregatesStates(JdbcStandardDeviation("marketplace_id"))
      correctlyAggregatesStates(JdbcUniqueness("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(JdbcDistinctness("attribute" :: Nil))
      correctlyAggregatesStates(JdbcCountDistinct("value" :: Nil))
      correctlyAggregatesStates(JdbcUniqueValueRatio("value"))
      correctlyAggregatesStates(JdbcCompleteness("attribute"))
      correctlyAggregatesStates(JdbcCompliance("attribute", "attribute like '%facets%'"))
      correctlyAggregatesStates(JdbcCorrelation("numbersA", "numbersB"))
      correctlyAggregatesStates(JdbcEntropy("attribute"))
      correctlyAggregatesStates(JdbcHistogram("value"))
    }
  }

  def correctlyAggregatesStates[S <: State[S]](analyzer: JdbcAnalyzer[S, Metric[_]])
  : Unit = {

    withJdbc { connection =>

      val dataA = initialData(connection)
      val dataB = deltaData(connection)
      val dataAB = completeData(connection)

      val stateA = analyzer.computeStateFrom(dataA)
      val stateB = analyzer.computeStateFrom(dataB)

      val metricFromCalculate = analyzer.calculate(dataAB)
      val mergedState = JdbcAnalyzers.merge(stateA, stateB)

      val metricFromAggregation = analyzer.computeMetricFrom(mergedState)

      assert(metricFromAggregation.instance == metricFromCalculate.instance)
      assert(metricFromAggregation.entity == metricFromCalculate.entity)
      assert(metricFromAggregation.name == metricFromCalculate.name)

      (metricFromAggregation.value.get, metricFromCalculate.value.get) match {
        case (aggResult: Number, calcResult: Number) =>
          assert(abs(aggResult.doubleValue() - calcResult.doubleValue()) <= 1e-15)
        case _ =>
          assert(metricFromAggregation.value == metricFromCalculate.value)
      }
      // assert(metricFromAggregation == metricFromCalculate)
    }
  }

  def initialData(connection: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
    "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")

    val data = Seq(
      Seq(1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended", 3, 12),
      Seq(1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics", 4, 951510),
      Seq(1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended", 5, 60),
      Seq(1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended", 6, 655),
      Seq(1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended", 7, 45),
      Seq(1, "B00DLT13JY", "Binding-0-value", "consumer_electronics", 49, 2012),
      Seq(1, "B00ICANXP4", "Binding-0-value", "pc", 50, 68),
      Seq(1, "B00MG1DSWI", "Binding-0-value", "toy", 51, 90),
      Seq(1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended", 52, 1),
      Seq(1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended", 53, 78),
      Seq(1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended", 54, 33),
      Seq(1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended", 27, 90),
      Seq(1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended", 28, 2),
      Seq(1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended", 29, 57),
      Seq(1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended", 1029, 39),
      Seq(1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended", 1030, 80),
      Seq(1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended", 13, 12),
      Seq(1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended", 17, 0),
      Seq(1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended", 55, 0),
      Seq(1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended", 83, 0))

    fillTableWithData("initialData", columns, data, connection)
  }

  def deltaData(connection: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
      "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")
    val data = Seq(
      Seq(1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products", 100, 7),
      Seq(1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products", 99, 8),
      Seq(1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products", 98, 9),
      Seq(1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products", 97, 10),
      Seq(1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products", 96, 11),
      Seq(1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products", 95, 12),
      Seq(1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products", 94, 13),
      Seq(1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products", 93, 14),
      Seq(1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products", 92, 15),
      Seq(1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products", 91, 18),
      Seq(1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products", 90, 25),
      Seq(1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products", 89, 27),
      Seq(1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products", 88, 28),
      Seq(1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products", 87, 29),
      Seq(1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products", 86, 30),
      Seq(1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products", 85, 60),
      Seq(1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products", 84, 62),
      Seq(1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products", 83, 70),
      Seq(1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products", 82, 71),
      Seq(1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products", 81, 72))

    fillTableWithData("deltaData", columns, data, connection)
  }

  def completeData(connection: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
      "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")

    val data = Seq(
      Seq(1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended", 3, 12),
      Seq(1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics", 4, 951510),
      Seq(1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended", 5, 60),
      Seq(1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended", 6, 655),
      Seq(1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended", 7, 45),
      Seq(1, "B00DLT13JY", "Binding-0-value", "consumer_electronics", 49, 2012),
      Seq(1, "B00ICANXP4", "Binding-0-value", "pc", 50, 68),
      Seq(1, "B00MG1DSWI", "Binding-0-value", "toy", 51, 90),
      Seq(1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended", 52, 1),
      Seq(1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended", 53, 78),
      Seq(1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended", 54, 33),
      Seq(1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended", 27, 90),
      Seq(1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended", 28, 2),
      Seq(1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended", 29, 57),
      Seq(1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended", 1029, 39),
      Seq(1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended", 1030, 80),
      Seq(1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended", 13, 12),
      Seq(1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended", 17, 0),
      Seq(1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended", 55, 0),
      Seq(1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended", 83, 0),
      Seq(1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products", 100, 7),
      Seq(1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products", 99, 8),
      Seq(1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products", 98, 9),
      Seq(1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products", 97, 10),
      Seq(1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products", 96, 11),
      Seq(1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products", 95, 12),
      Seq(1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products", 94, 13),
      Seq(1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products", 93, 14),
      Seq(1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products", 92, 15),
      Seq(1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products", 91, 18),
      Seq(1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products", 90, 25),
      Seq(1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products", 89, 27),
      Seq(1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products", 88, 28),
      Seq(1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products", 87, 29),
      Seq(1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products", 86, 30),
      Seq(1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products", 85, 60),
      Seq(1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products", 84, 62),
      Seq(1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products", 83, 70),
      Seq(1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products", 82, 71),
      Seq(1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products", 81, 72))

    fillTableWithData("completeData", columns, data, connection)
  }
}
