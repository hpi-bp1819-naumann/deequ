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
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.runtime.jdbc.executor.EmptyStateException
import com.amazon.deequ.runtime.jdbc.operators.JdbcColumn._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable
import scala.util.Success

class NullHandlingTests extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  private[this] def dataWithNullColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("stringCol", StringType) ::
        JdbcStructField("numericCol", DoubleType) ::
        JdbcStructField("numericCol2", DoubleType) ::
        JdbcStructField("numericCol3", DoubleType) :: Nil)

    val data = Seq(
      Seq(null, null, null, 1.0),
      Seq(null, null, null, 2.0),
      Seq(null, null, null, 3.0),
      Seq(null, null, null, 4.0),
      Seq(null, null, null, 5.0),
      Seq(null, null, null, 6.0),
      Seq(null, null, null, 7.0),
      Seq(null, null, null, 8.0))

    fillTableWithData("dataWithNullColumns", schema, data, connection)
  }

  "Null schema" should {

    "produce correct states" in withJdbc { connection =>

      val data = dataWithNullColumns(connection)

      SizeOp().computeStateFrom(data) shouldBe Some(NumMatches(8))
      CompletenessOp("stringCol").computeStateFrom(data) shouldBe Some(NumMatchesAndCount(0, 8))

      MeanOp("numericCol").computeStateFrom(data) shouldBe None
      StandardDeviationOp("numericCol").computeStateFrom(data) shouldBe None
      MinimumOp("numericCol").computeStateFrom(data) shouldBe None
      MaximumOp("numericCol").computeStateFrom(data) shouldBe None

      DataTypeOp("stringCol").computeStateFrom(data) shouldBe
        Some(DataTypeHistogram(8L, 0L, 0L, 0L, 0L))

      SumOp("numericCol").computeStateFrom(data) shouldBe None
      //ApproxQuantileOp("numericCol", 0.5).computeStateFrom(data) shouldBe None

      val stringColFrequenciesAndNumRows = CountDistinctOp("stringCol").computeStateFrom(data)
      assert(stringColFrequenciesAndNumRows.isDefined)

      stringColFrequenciesAndNumRows.get.numRows shouldBe 8
      println(stringColFrequenciesAndNumRows.get.frequencies())
      // this differs from spark because we also store the number of null values in the table
      stringColFrequenciesAndNumRows.get.frequencies()._2.keys.size shouldBe 1L // TODO 0L

      /*val numericColFrequenciesAndNumRows = MutualInformationOp("numericCol", "numericCol2").computeStateFrom(data)

      assert(numericColFrequenciesAndNumRows.isDefined)

      numericColFrequenciesAndNumRows.get.numRows shouldBe 8
      numericColFrequenciesAndNumRows.get.frequencies.count() shouldBe 0L*/


      CorrelationOp("numericCol", "numericCol2").computeStateFrom(data) shouldBe None
    }

    "produce correct metrics" in withJdbc { connection =>

      val data = dataWithNullColumns(connection)

      SizeOp().calculate(data).value shouldBe Success(8.0)
      CompletenessOp("stringCol").calculate(data).value shouldBe Success(0.0)

      assertFailedWithEmptyState(MeanOp("numericCol").calculate(data))

      assertFailedWithEmptyState(StandardDeviationOp("numericCol").calculate(data))
      assertFailedWithEmptyState(MinimumOp("numericCol").calculate(data))
      assertFailedWithEmptyState(MaximumOp("numericCol").calculate(data))

      val dataTypeDistribution = DataTypeOp("stringCol").calculate(data).value.get
      dataTypeDistribution.values("Unknown").ratio shouldBe 1.0

      assertFailedWithEmptyState(SumOp("numericCol").calculate(data))
      //assertFailedWithEmptyState(ApproxQuantileOp("numericCol", 0.5).calculate(data))

      CountDistinctOp("stringCol").calculate(data).value shouldBe Success(0.0)
      //ApproxCountDistinctOp("stringCol").calculate(data).value shouldBe Success(0.0)

      assertFailedWithEmptyState(EntropyOp("stringCol").calculate(data))
      //assertFailedWithEmptyState(MutualInformationOp("numericCol", "numericCol2").calculate(data))
      //assertFailedWithEmptyState(MutualInformationOp("numericCol", "numericCol3").calculate(data))
      assertFailedWithEmptyState(CorrelationOp("numericCol", "numericCol2").calculate(data))
      assertFailedWithEmptyState(CorrelationOp("numericCol", "numericCol3").calculate(data))
    }

    "include analyzer name in EmptyStateExceptions" in withJdbc { connection =>

      val data = dataWithNullColumns(connection)

      val metricResult = MeanOp("numericCol").calculate(data).value

      assert(metricResult.isFailure)

      val exceptionMessage = metricResult.failed.get.getMessage

      assert(exceptionMessage == "Empty state for operator MeanOp(numericCol,None), " +
        "all input values were NULL.")

    }
  }

  private[this] def assertFailedWithEmptyState(metric: DoubleMetric): Unit = {
    assert(metric.value.isFailure)
    assert(metric.value.failed.get.isInstanceOf[EmptyStateException])
  }
}
