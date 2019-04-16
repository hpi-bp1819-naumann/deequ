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
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.JdbcHelpers
import com.amazon.deequ.runtime.jdbc.operators.FrequencyBasedOperatorsUtils.uniqueTableName
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class StateAggregationTests extends WordSpec with Matchers with JdbcContextSpec
  with FixtureSupport {

  "State aggregation outside" should {

    "give correct results" in withJdbc { connection =>

      correctlyAggregatesStates(connection, SizeOp())
      correctlyAggregatesStates(connection, UniquenessOp("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(connection, DistinctnessOp("attribute" :: Nil))
      correctlyAggregatesStates(connection, CountDistinctOp("value" :: Nil))
      correctlyAggregatesStates(connection, UniqueValueRatioOp("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(connection, CompletenessOp("attribute"))
      correctlyAggregatesStates(connection, ComplianceOp("attribute", "attribute like '%facets%'"))
      // TODO approx correctlyAggregatesStates(connection, ApproxCountDistinctOp("attribute"))
      // TODO mutual correctlyAggregatesStates(connection, MutualInformationOp("numbersA", "numbersB"))
      correctlyAggregatesStates(connection, CorrelationOp("numbersA", "numbersB"))
    }
  }

  def correctlyAggregatesStates[S <: State[S]](
      connection: Connection,
      analyzer: Operator[S, Metric[_]])
    : Unit = {

    val dataA = initialData(connection)
    val dataB = deltaData(connection)
    val dataAB = dataA union dataB

    val stateA = analyzer.computeStateFrom(dataA)
    val stateB = analyzer.computeStateFrom(dataB)

    val metricFromCalculate = analyzer.calculate(dataAB)
    val mergedState = Operators.merge(stateA, stateB)

    val metricFromAggregation = analyzer.computeMetricFrom(mergedState)

    assert(metricFromAggregation.instance == metricFromCalculate.instance)
    assert(metricFromAggregation.entity == metricFromCalculate.entity)
    assert(metricFromAggregation.name == metricFromCalculate.name)
    assert(metricFromAggregation.value.isSuccess == metricFromCalculate.value.isSuccess)
    assert(
      math.abs(
        metricFromAggregation.value.get.asInstanceOf[Double] -
          metricFromCalculate.value.get.asInstanceOf[Double]) < 1e-8)

    //assert(metricFromAggregation == metricFromCalculate)
  }

  def initialData(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("marketplace_id", IntegerType) ::
        JdbcStructField("item", StringType) ::
        JdbcStructField("attribute", StringType) ::
        JdbcStructField("value", StringType) :: Nil)

    val data =
      Seq(
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

    JdbcHelpers.fillTableWithData(uniqueTableName(), schema, data, connection)
        .withColumn("numbersA", FloatType, Some("RANDOM()"))
        .withColumn("numbersB", FloatType, Some("RANDOM()"))
  }

  def deltaData(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("marketplace_id", IntegerType) ::
        JdbcStructField("item", StringType) ::
        JdbcStructField("attribute", StringType) ::
        JdbcStructField("value", StringType) :: Nil)

    val data =
      Seq(
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

    JdbcHelpers.fillTableWithData(uniqueTableName(), schema, data, connection)
      .withColumn("numbersA", FloatType, Some("RANDOM()"))
      .withColumn("numbersB", FloatType, Some("RANDOM()"))
  }
}
