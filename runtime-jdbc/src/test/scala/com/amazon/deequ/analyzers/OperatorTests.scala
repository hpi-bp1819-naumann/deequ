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

package com.amazon.deequ
package analyzers

import com.amazon.deequ.metrics.{Distribution, DistributionValue, DoubleMetric, Entity, HistogramMetric}
import com.amazon.deequ.runtime.jdbc._
import com.amazon.deequ.runtime.jdbc.executor.NoSuchColumnException
import com.amazon.deequ.runtime.jdbc.operators.JdbcColumn._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.statistics.{DataTypeInstances, Patterns}
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable
import scala.util.{Failure, Success}

class OperatorTests extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  "Size analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val (tableMissing, tableMissingSize) = getTableMissingWithSize(connection)
      val (tableFull, tableFullSize) = getTableFullWithSize(connection)

      assert(SizeOp().calculate(tableMissing) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableMissingSize)))
      assert(SizeOp().calculate(tableFull) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableFullSize)))
    }
  }

  "Completeness analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      assert(CompletenessOp("someMissingColumn").preconditions.size == 1,
        "should check column name availability")
      assert(CompletenessOp("att1").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.5)))
      assert(CompletenessOp("att2").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att2", Success(0.75)))

    }
    "fail on wrong column input" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      CompletenessOp("someMissingColumn").calculate(tableMissing) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Completeness")
          assert(metric.instance == "someMissingColumn")
          assert(metric.value.isFailure)
      }
    }

    "work with filtering" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      val result = CompletenessOp("att1", Some("item IN ('1', '2')")).calculate(tableMissing)
      assert(result == DoubleMetric(Entity.Column, "Completeness", "att1", Success(1.0)))
    }

  }

  "Jdbc UniquenessOp" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(UniquenessOp("att1").calculate(tableMissing) == DoubleMetric(Entity.Column, "Uniqueness",
        "att1", Success(0.0)))
      assert(UniquenessOp("att2").calculate(tableMissing) == DoubleMetric(Entity.Column, "Uniqueness",
        "att2", Success(0.0)))


      assert(UniquenessOp("att1").calculate(tableFull) == DoubleMetric(Entity.Column, "Uniqueness",
        "att1", Success(0.25)))
      assert(UniquenessOp("att2").calculate(tableFull) == DoubleMetric(Entity.Column, "Uniqueness",
        "att2", Success(0.25)))

    }
    "compute correct metrics on multi schema" in withJdbc { connection =>
      val tableFull = getTableWithUniqueColumns(connection)

      assert(UniquenessOp("uniqueValues").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueValues", Success(1.0)))
      assert(UniquenessOp("uniqueWithNulls").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueWithNulls", Success(5 / 6.0)))
      assert(UniquenessOp(Seq("uniqueValues", "nonUniqueValues")).calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "uniqueValues,nonUniqueValues", Success(1.0)))
      assert(UniquenessOp(Seq("uniqueValues", "nonUniqueWithNulls")).calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "uniqueValues,nonUniqueWithNulls",
          Success(3 / 6.0)))
      assert(UniquenessOp(Seq("nonUniqueValues", "onlyUniqueWithOtherNonUnique")).calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "nonUniqueValues,onlyUniqueWithOtherNonUnique",
          Success(1.0)))

    }
    "fail on wrong column input" in withJdbc { connection =>
      val tableFull = getTableWithUniqueColumns(connection)

      UniquenessOp("nonExistingColumn").calculate(tableFull) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }

      UniquenessOp(Seq("nonExistingColumn", "unique")).calculate(tableFull) match {
        case metric =>
          assert(metric.entity == Entity.Mutlicolumn)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn,unique")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }
    }
  }

  "Entropy analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableFull = getTableFull(connection)

      assert(EntropyOp("att1").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
      assert(EntropyOp("att2").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))

    }
  }

  /* "MutualInformation analyzer" should {
    "compute correct metrics " in withJdbc { connection =>
      val tableFull = getTableFull(connection)
      assert(MutualInformationOp("att1", "att2").calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "MutualInformation", "att1,att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
    }
    "yields 0 for conditionally uninformative schema" in withJdbc { connection =>
      val table = getTableWithConditionallyUninformativeColumns(connection)
      assert(MutualInformationOp("att1", "att2").calculate(table).value == Success(0.0))
    }
    "compute entropy for same column" in withJdbc { session =>
      val data = getTableFull(session)

      val entropyViaMI = MutualInformationOp("att1", "att1").calculate(data)
      val entropy = EntropyOp("att1").calculate(data)

      assert(entropyViaMI.value.isSuccess)
      assert(entropy.value.isSuccess)

      assert(entropyViaMI.value.get == entropy.value.get)
    }
  } */

  "Compliance analyzer" should {
    "compute correct metrics " in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      assert(ComplianceOp("rule1", "att1 > 3").calculate(table) ==
        DoubleMetric(Entity.Column, "Compliance", "rule1", Success(3.0 / 6)))
      assert(ComplianceOp("rule2", "att1 > 2").calculate(table) ==
        DoubleMetric(Entity.Column, "Compliance", "rule2", Success(4.0 / 6)))

    }

    "compute correct metrics with filtering" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = ComplianceOp("rule1", "att2 = 0", Some("att1 < 4")).calculate(table)
      assert(result == DoubleMetric(Entity.Column, "Compliance", "rule1", Success(1.0)))
    }

    "fail on wrong column input" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      ComplianceOp("rule1", "attNoSuchColumn > 3").calculate(table) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Compliance")
          assert(metric.instance == "rule1")
          assert(metric.value.isFailure)
      }

    }
  }


  "Histogram analyzer" should {
    "compute correct metrics " in withJdbc { connection =>
      val tableFull = getTableMissing(connection)
      val histogram = HistogramOp("att1").calculate(tableFull)
      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 3)
          assert(hv.values.keys == Set("a", "b", HistogramOp.NullFieldReplacement))

      }
    }
    /* "compute correct metrics after binning if provided" in withJdbc { connection =>
      val customBinner = utable {
        (cnt: String) =>
          cnt match {
            case "a" | "b" => "Value1"
            case _ => "Value2"
          }
      }
      val tableFull = getTableMissing(connection)
      val histogram = HistogramOp("att1", Some(customBinner)).calculate(tableFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 2)
          assert(hv.values.keys == Set("Value1", "Value2"))

      }
    } */
    "compute correct metrics should only get top N bins" in withJdbc { connection =>
      val tableFull = getTableMissing(connection)
      val histogram = HistogramOp("att1", None, 2).calculate(tableFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 2)
          assert(hv.values.keys == Set("a", HistogramOp.NullFieldReplacement))

      }
    }

    "fail for max detail bins > 1000" in withJdbc { connection =>
      val table = getTableFull(connection)
      HistogramOp("att1", binningUdf = None, maxDetailBins = 1001).calculate(table).value match {
        case Failure(t) => t.getMessage shouldBe "Cannot return " +
          "histogram values for more than 1000 values"
        case _ => fail("test was expected to fail due to parameter precondition")
      }
    }
  }

  "Jdbc data type operator" should {

    def distributionFrom(
        nonZeroValues: (DataTypeInstances.Value, DistributionValue)*)
      : Distribution = {

      val nonZeroValuesWithStringKeys = nonZeroValues.toSeq
        .map { case (instance, distValue) => instance.toString -> distValue }

      val dataTypes = DataTypeInstances.values.map { _.toString }

      val zeros = dataTypes
        .diff { nonZeroValuesWithStringKeys.map { case (distKey, _) => distKey }.toSet }
        .map(dataType => dataType -> DistributionValue(0, 0.0))
        .toSeq

      val distributionValues = Map(zeros ++ nonZeroValuesWithStringKeys: _*)

      Distribution(distributionValues, numberOfBins = dataTypes.size)
    }

    "fall back to String in case no known data type matched" in withJdbc { connection =>
      val table = getTableFull(connection)

      DataTypeOp("att1").calculate(table).value shouldBe
        Success(distributionFrom(DataTypeInstances.String -> DistributionValue(4, 1.0)))
    }

    "detect integral type correctly" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(6, 1.0))
      DataTypeOp("att1").calculate(table).value shouldBe Success(expectedResult)
    }

    "detect integral type correctly for negative numbers" in withJdbc { connection =>
      val table = getTableWithNegativeNumbers(connection)
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(4, 1.0))
      DataTypeOp("att1").calculate(table).value shouldBe Success(expectedResult)
    }

    "detect fractional type correctly for negative numbers" in withJdbc { connection =>
      val table = getTableWithNegativeNumbers(connection)
      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(4, 1.0))
      DataTypeOp("att2").calculate(table).value shouldBe Success(expectedResult)
    }

    "compute correct metrics" in withJdbc { connection =>

      withJdbc { connection =>
        val table = getTableWithImproperDataTypes(connection)

        assert(DataTypeOp("mixed",
          Some("type_integer IS NOT NULL")).calculate(table) == HistogramMetric("mixed",
          Success(Distribution(Map(
            "Boolean" -> DistributionValue(1, 0.2),
            "Fractional" -> DistributionValue(1, 0.2),
            "Integral" -> DistributionValue(1, 0.2),
            "Unknown" -> DistributionValue(0, 0),
            "String" -> DistributionValue(2, 0.4)), 5)))
        )
      }
    }

    "detect fractional type correctly" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
        .withColumn("att1_float", FloatType, Some(s"CAST(att1 AS ${FloatType.toString()})"))

      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(6, 1.0))
        DataTypeOp("att1_float").calculate(table).value shouldBe Success(expectedResult)
    }

    "detect integral type in string column" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
        .withColumn("att1_str", StringType, Some(s"CAST(att1 AS $StringType)"))
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(6, 1.0))
      DataTypeOp("att1_str").calculate(table).value shouldBe Success(expectedResult)
    }

    "detect fractional type in string column" in withJdbc { connection =>
      val table = getTableWithNumericFractionalValues(connection)
        .withColumn("att1_str", StringType, Some(s"CAST(att1 AS $StringType)"))

      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(6, 1.0))
      DataTypeOp("att1_str").calculate(table).value shouldBe Success(expectedResult)
    }

    "fall back to string in case the string column didn't match " +
      " any known other data type" in withJdbc { connection =>
      val table = getTableFull(connection)
      val expectedResult = distributionFrom(DataTypeInstances.String -> DistributionValue(4, 1.0))
      DataTypeOp("att1").calculate(table).value shouldBe Success(expectedResult)
    }

    "detect fractional for mixed fractional and integral" in withJdbc { connection =>
      val table = getTableFractionalIntegralTypes(connection)
      DataTypeOp("att1").calculate(table).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.5),
          DataTypeInstances.Integral -> DistributionValue(1, 0.5)
        )
      )
    }

    "fall back to string for mixed fractional and string" in withJdbc { connection =>
      val table = getTableFractionalStringTypes(connection)
      DataTypeOp("att1").calculate(table).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.5),
          DataTypeInstances.String -> DistributionValue(1, 0.5)
        )
      )
    }

    "fall back to string for mixed integral and string" in withJdbc { connection =>
      val table = getTableIntegralStringTypes(connection)
      DataTypeOp("att1").calculate(table).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Integral -> DistributionValue(1, 0.5),
          DataTypeInstances.String -> DistributionValue(1, 0.5)
        )
      )
    }

    "integral for numeric and null" in withJdbc { connection =>
      val table = getTableWithUniqueColumns(connection)
      DataTypeOp("uniqueWithNulls").calculate(table).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Unknown -> DistributionValue(1, 1.0/6.0),
          DataTypeInstances.Integral -> DistributionValue(5, 5.0/6.0)
        )
      )
    }

    "detect boolean type" in withJdbc { connection =>

      val schema = JdbcStructType(
        JdbcStructField("item", StringType) ::
          JdbcStructField("att1", StringType) :: Nil)

      val data = Seq(
        Seq("1", "true"),
        Seq("2", "false"))
      val table = fillTableWithData("booleanAttribute", schema, data, connection)

      val expectedResult = distributionFrom(DataTypeInstances.Boolean -> DistributionValue(2, 1.0))

      DataTypeOp("att1").calculate(table).value shouldBe Success(expectedResult)
    }

    "fall back to string for boolean and null" in withJdbc { connection =>

      val schema = JdbcStructType(
        JdbcStructField("item", StringType) ::
          JdbcStructField("att1", StringType) :: Nil)

      val data = Seq(
        Seq("1", "true"),
        Seq("2", "false"),
        Seq("3", null),
        Seq("4", "2.0")
      )
      val table = fillTableWithData("booleanAndNullAttribute", schema, data, connection)

      DataTypeOp("att1").calculate(table).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.25),
          DataTypeInstances.Unknown -> DistributionValue(1, 0.25),
          DataTypeInstances.Boolean -> DistributionValue(2, 0.5)
        )
      )
    }
  }

  "Basic statistics" should {
    "compute mean correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = MeanOp("att1").calculate(table).value
      result shouldBe Success(3.5)
    }
    "fail to compute mean for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(MeanOp("att1").calculate(table).value.isFailure)
    }
    "compute mean correctly for numeric data with where predicate" in
      withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = MeanOp("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(3.0)
      }

    "compute standard deviation correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = StandardDeviationOp("att1").calculate(table).value
      result shouldBe Success(1.707825127659933)
    }
    "fail to compute standard deviaton for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(StandardDeviationOp("att1").calculate(table).value.isFailure)
    }

    "compute minimum correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = MinimumOp("att1").calculate(table).value
      result shouldBe Success(1.0)
    }
    "fail to compute minimum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(MinimumOp("att1").calculate(table).value.isFailure)
    }

    "compute maximum correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = MaximumOp("att1").calculate(table).value
      result shouldBe Success(6.0)
    }

    "compute maximum correctly for numeric data with filtering" in
      withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = MaximumOp("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(5.0)
      }

    "fail to compute maximum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(MaximumOp("att1").calculate(table).value.isFailure)
    }

    "compute sum correctly for numeric data" in withJdbc { session =>
      val table = getTableWithNumericValues(session)
      SumOp("att1").calculate(table).value shouldBe Success(21)
    }

    "fail to compute sum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(SumOp("att1").calculate(table).value.isFailure)
    }

    "should work correctly on decimal schema" in withJdbc { connection =>

      val schema = JdbcStructType(
        JdbcStructField("num", DecimalType) :: Nil)

      val data = Seq(
        Seq(BigDecimal(123.45)),
        Seq(BigDecimal(99)),
        Seq(BigDecimal(678)))
      val table = fillTableWithData("decimalColumn", schema, data, connection)

      val result = MinimumOp("num").calculate(table)

      assert(result.value.isSuccess)
      assert(result.value.get == 99.0)
    }
  }

  "Count distinct analyzers" should {
    /* "compute approximate distinct count for numeric data" in withJdbc { connection =>
      val table = getTableWithUniqueColumns(connection)
      val result = ApproxCountDistinctOp("uniqueWithNulls").calculate(table).value

      result shouldBe Success(5.0)
    }

    "compute approximate distinct count for numeric data with filtering" in
      withJdbc { connection =>

        val table = getTableWithUniqueColumns(connection)
        val result = ApproxCountDistinctOp("uniqueWithNulls", where = Some("unique < 4"))
          .calculate(table).value
        result shouldBe Success(2.0)
      } */

    "compute exact distinct count of elements for numeric data" in withJdbc {
      connection =>
        val table = getTableWithUniqueColumns(connection)
        val result = CountDistinctOp("uniqueWithNulls").calculate(table).value
        result shouldBe Success(5.0)
      }
  }

  /* "Approximate quantile analyzer" should {

    "approximate quantile 0.5 within acceptable error bound" in
      withJdbc { connection =>

        import connection.implicits._
        val table = connection.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantileOp("att1", 0.5).calculate(table).value.get

        assert(result > -20 && result < 20)
      }

    "approximate quantile 0.25 within acceptable error bound" in
      withJdbc { connection =>

        import connection.implicits._
        val table = connection.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantileOp("att1", 0.25).calculate(table).value.get

        assert(result > -520 && result < -480)
      }

    "approximate quantile 0.75 within acceptable error bound" in
      withJdbc { connection =>

        import connection.implicits._
        val table = connection.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantileOp("att1", 0.75).calculate(table).value.get

        assert(result > 480 && result < 520)
      }

    "fail for relative error > 1.0" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      ApproxQuantileOp("att1", quantile = 0.5, relativeError = 1.1).calculate(table).value match {
        case Failure(t) => t.getMessage shouldBe "Relative error parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: 1.1!"
        case _ => fail(OperatorTests.expectedPreconditionViolation)
      }
    }
    "fail for relative error < 0.0" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      ApproxQuantileOp("att1", quantile = 0.5, relativeError = -0.1).calculate(table).value match {
        case Failure(t) => t.getMessage shouldBe "Relative error parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: -0.1!"
        case _ => fail(OperatorTests.expectedPreconditionViolation)
      }
    }
    "fail for quantile < 0.0" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      ApproxQuantileOp("att1", quantile = -0.1).calculate(table).value match {
        case Failure(t) => t.getMessage shouldBe "Quantile parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: -0.1!"
        case _ => fail(OperatorTests.expectedPreconditionViolation)

      }
    }
    "fail for quantile > 1.0" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      ApproxQuantileOp("att1", quantile = 1.1).calculate(table).value match {
        case Failure(t) => t.getMessage shouldBe "Quantile parameter must be " +
          "in the closed interval [0, 1]. Currently, the value is: 1.1!"
        case _ => fail(OperatorTests.expectedPreconditionViolation)
      }
    }
  } */

  "Pearson correlation" should {
    "yield NaN for conditionally uninformative schema" in withJdbc { connection =>
      val table = getTableWithConditionallyUninformativeColumns(connection)
      val corr = CorrelationOp("att1", "att2").calculate(table).value.get
      assert(java.lang.Double.isNaN(corr))
    }
    "yield 1.0 for maximal conditionally informative schema" in withJdbc { connection =>
      val table = getTableWithConditionallyInformativeColumns(connection)
      CorrelationOp("att1", "att2").calculate(table) shouldBe DoubleMetric(
        Entity.Mutlicolumn,
        "Correlation",
        "att1,att2",
        Success(1.0)
      )
    }
    "is commutative" in withJdbc { connection =>
      val table = getTableWithConditionallyInformativeColumns(connection)
      CorrelationOp("att1", "att2").calculate(table).value shouldBe
        CorrelationOp("att2", "att1").calculate(table).value
    }
  }


  "Pattern compliance analyzer" should {
    val someColumnName = "some"

    "match doubles in nullable column" in withJdbc { connection =>
      val table = tableWithColumn(someColumnName, DoubleType, connection, Seq(1.1),
        Seq(null), Seq(3.2), Seq(4.4))

      println(table.rows())

      PatternMatchOp(someColumnName, """\d\.\d""".r).calculate(table).value shouldBe Success(0.75)
    }

    "match integers in a String column" in withJdbc { connection =>
      val table = tableWithColumn(someColumnName, StringType, connection, Seq("1"), Seq("a"))
      PatternMatchOp(someColumnName, """\d""".r).calculate(table).value shouldBe Success(0.5)
    }

    "match email addresses" in withJdbc { connection =>
      val table = tableWithColumn(someColumnName, StringType, connection,
        Seq("someone@somewhere.org"), Seq("someone@else"))
      PatternMatchOp(someColumnName, Patterns.EMAIL).calculate(table).value shouldBe Success(0.5)
    }

    "match credit card numbers" in withJdbc { connection =>
      // https://www.paypalobjects.com/en_AU/vhelp/paypalmanager_help/credit_card_numbers.htm
      val maybeCreditCardNumbers = Seq(
        "378282246310005",// AMEX

        "6011111111111117", // Discover
        "6011 1111 1111 1117", // Discover spaced
        "6011-1111-1111-1117", // Discover dashed

        "5555555555554444", // MasterCard
        "5555 5555 5555 4444", // MasterCard spaced
        "5555-5555-5555-4444", // MasterCard dashed

        "4111111111111111", // Visa
        "4111 1111 1111 1111", // Visa spaced
        "4111-1111-1111-1111", // Visa dashed

        "0000111122223333", // not really a CC number
        "000011112222333",  // not really a CC number
        "00001111222233"    // not really a CC number
      )
      val table = tableWithColumn(someColumnName, StringType, connection,
        maybeCreditCardNumbers.map(Seq(_)): _*)
      val analyzer = PatternMatchOp(someColumnName, Patterns.CREDITCARD)

      analyzer.calculate(table).value shouldBe Success(10.0/13.0)
    }

    "match URLs" in withJdbc { connection =>
      // URLs taken from https://mathiasbynens.be/demo/url-regex
      val maybeURLs = Seq(
        "http://foo.com/blah_blah",
        "http://foo.com/blah_blah_(wikipedia)",
        "http://foo.bar/?q=Test%20URL-encoded%20stuff",

        // scalastyle:off
        "http://➡.ws/䨹",
        "http://⌘.ws/",
        "http://☺.damowmow.com/",
        "http://例子.测试",
        // scalastyle:on

        "https://foo_bar.example.com/",
        "http://userid@example.com:8080",
        "http://foo.com/blah_(wikipedia)#cite-1",

        "http://../", // not really a valid URL
        "h://test",  // not really a valid URL
        "http://.www.foo.bar/"    // not really a valid URL
      )
      val table = tableWithColumn(someColumnName, StringType, connection,
        maybeURLs.map(Seq(_)): _*)
      val analyzer = PatternMatchOp(someColumnName, Patterns.URL)
      analyzer.calculate(table).value shouldBe Success(10.0/13.0)
    }

    "match US social security numbers" in withJdbc { connection =>
      // https://en.wikipedia.org/wiki/Social_Security_number#Valid_SSNs
      val maybeSSN = Seq(
        "111-05-1130",
        "111051130", // without dashes
        "111-05-000", // no all-zeros allowed in any group
        "111-00-000", // no all-zeros allowed in any group
        "000-05-1130", // no all-zeros allowed in any group
        "666-05-1130", // 666 and 900-999 forbidden in first digit group
        "900-05-1130", // 666 and 900-999 forbidden in first digit group
        "999-05-1130" // 666 and 900-999 forbidden in first digit group
      )
      val table = tableWithColumn(someColumnName, StringType, connection,
        maybeSSN.map(Seq(_)): _*)
      val analyzer = PatternMatchOp(someColumnName, Patterns.SOCIAL_SECURITY_NUMBER_US)
      analyzer.calculate(table).value shouldBe Success(2.0 / 8.0)
    }
  }
}

object OperatorTests {
  val expectedPreconditionViolation: String =
    "computation was unexpectedly successful, should have failed due to violated precondition"
}
