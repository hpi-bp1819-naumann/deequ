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

import java.sql.Connection

import com.amazon.deequ.JdbcContextSpec
import com.amazon.deequ.jdbc.OperatorList
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{InMemoryJdbcStateProvider, JdbcHelpers}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class IncrementalAnalysisTest extends WordSpec with Matchers with JdbcContextSpec
  with FixtureSupport {

  "An IncrementalAnalysisRunner" should {
    "produce the same results as a non-incremental analysis" in withJdbc { connection =>

      val initial = initialData(connection)
      val delta = deltaData(connection)

      val everything = initial.union(delta)

      val analysis = OperatorList().addAnalyzers(
        Seq(SizeOp(),
          UniquenessOp("marketplace_id"),
          CompletenessOp("item"),
          EntropyOp("attribute"),
          CompletenessOp("attribute"),
          EntropyOp("value")))

      val initialStates = InMemoryJdbcStateProvider()

      analysis.run(initial, saveStatesWith = Some(initialStates))
      val incrementalResults = analysis.run(delta, aggregateWith = Some(initialStates))

      val nonIncrementalResults = analysis.run(everything)

      nonIncrementalResults.allMetrics.foreach { println }
      println("\n")
      incrementalResults.allMetrics.foreach { println }



      assert(incrementalResults == nonIncrementalResults)
    }

    "produce correct results when sharing scans for aggregation functions" in
      withJdbc { connection =>

        val initial = initialData(connection)
        val delta = deltaData(connection)
        val everything = initial.union(delta)

        val initialStates = InMemoryJdbcStateProvider()

        val analyzers = Seq(
          ComplianceOp("attributeNonNull", "attribute IS NOT NULL"),
          ComplianceOp("categoryAttribute", "attribute LIKE 'CATEGORY%'"),
          ComplianceOp("attributeKeyword", "attribute LIKE '%keyword%'"),
          CompletenessOp("marketplace_id"),
          CompletenessOp("item"))

        val analysis = OperatorList(analyzers)

        analysis.run(initial, saveStatesWith = Some(initialStates))
        val results = analysis.run(delta, aggregateWith = Some(initialStates))

        results.metricMap.foreach { case (analyzer, metric) =>
          val nonIncrementalMetric = analyzer.calculate(everything)
          assert(nonIncrementalMetric == metric)
        }
      }

    "produce correct results when sharing scans for histogram-based metrics" in
      withJdbc { connection =>

        val initial = initialData(connection)
        val delta = deltaData(connection)
        val everything = initial.union(delta)

        val analysis = OperatorList(UniquenessOp("value") :: EntropyOp("value") :: Nil)

        val initialStates = InMemoryJdbcStateProvider()

        analysis.run(initial, saveStatesWith = Some(initialStates))
        val results = analysis.run(delta, aggregateWith = Some(initialStates))

        results.metricMap.foreach { case (analyzer, metric) =>
          val nonIncrementalMetric = analyzer.calculate(everything)
          assert(nonIncrementalMetric == metric)
        }
      }

  }

  def initialData(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("marketplace_id", IntegerType) ::
        JdbcStructField("item", StringType) ::
        JdbcStructField("attribute", StringType) ::
        JdbcStructField("value", StringType) :: Nil)

    val data = Seq(
      Seq(1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended"),
      Seq(1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics"),
      Seq(1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended"),
      Seq(1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended"),
      Seq(1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended"),
      Seq(1, "B00DLT13JY", "Binding-0-value", "consumer_electronics"),
      Seq(1, "B00ICANXP4", "Binding-0-value", "pc"),
      Seq(1, "B00MG1DSWI", "Binding-0-value", "toy"),
      Seq(1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended"),
      Seq(1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended"))

    JdbcHelpers.fillTableWithData("initialData", schema, data, connection)
  }

  def deltaData(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("marketplace_id", IntegerType) ::
        JdbcStructField("item", StringType) ::
        JdbcStructField("attribute", StringType) ::
        JdbcStructField("value", StringType) :: Nil)

    val data = Seq(
      Seq(1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products"),
      Seq(1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products"),
      Seq(1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products"),
      Seq(1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products"))

    JdbcHelpers.fillTableWithData("deltaData", schema, data, connection)
  }
}
