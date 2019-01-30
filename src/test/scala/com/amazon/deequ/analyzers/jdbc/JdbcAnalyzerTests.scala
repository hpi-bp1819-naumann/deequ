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

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.NoSuchColumnException
import com.amazon.deequ.metrics._
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class JdbcAnalyzerTests
  extends WordSpec with Matchers with JdbcContextSpec with JdbcFixtureSupport {

  "DataType analyzer" should {
    "compute correct metrics" in withJdbc { connection =>

      val table = getTableWithImproperDataTypes(connection)

      assert(DataType("mixed",
        Some("type_integer IS NOT NULL")).calculate(table) == HistogramMetric("mixed",
        Success(Distribution(Map(
          "Boolean" -> DistributionValue(1, 0.2),
          "Fractional" -> DistributionValue(1, 0.2),
          "Integral" -> DistributionValue(1, 0.2),
          "Unknown" -> DistributionValue(1, 0.2),
          "String" -> DistributionValue(1, 0.2)), 5)))
      )
      assert(DataType("type_integer",
        Some("type_integer IS NOT NULL")).calculate(table) == HistogramMetric("type_integer",
        Success(Distribution(Map(
          "Boolean" -> DistributionValue(0, 0.0),
          "Fractional" -> DistributionValue(0, 0.0),
          "Integral" -> DistributionValue(5, 1.0),
          "Unknown" -> DistributionValue(0, 0.0),
          "String" -> DistributionValue(0, 0.0)), 5)))
      )
      assert(DataType("type_fractional",
        Some("type_integer IS NOT NULL")).calculate(table) == HistogramMetric("type_fractional",
        Success(Distribution(Map(
          "Boolean" -> DistributionValue(0, 0.0),
          "Fractional" -> DistributionValue(4, 0.8),
          "Integral" -> DistributionValue(0, 0.0),
          "Unknown" -> DistributionValue(1, 0.2),
          "String" -> DistributionValue(0, 0.0)), 5)))
      )
    }
  }

  "PatternMatch analyzer" should {
    "compute correct metrics" in withJdbc { connection =>

      val table = getTableWithImproperDataTypes(connection)

      assert(PatternMatch("mixed",
        """^\s*(?:-|\+)?\d+\.\d+\s*$""".r,
        Some("type_integer IS NOT NULL")).calculate(table) == DoubleMetric(
        Entity.Column, "PatternMatch", "mixed", Success(0.2))
      )
    }
  }

  "Size analyzer" should {
    "compute correct metrics" in withJdbc { connection =>

      val tableMissing = getTableMissingWithSize(connection)
      val tableFull = getTableFullWithSize(connection)
      val tableFiltered = getTableFullWithSize(connection)
      val tableEmpty = getTableEmptyWithSize(connection)

      assert(Size().calculate(tableMissing._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableMissing._2)))
      assert(Size().calculate(tableFull._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableFull._2)))
      assert(Size(where = Some("item != 3")).calculate(tableFiltered._1) ==
        DoubleMetric(Entity.Dataset, "Size", "*", Success(3.0)))
      assert(Size().calculate(tableEmpty._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableEmpty._2)))

    }
  }

  "Completeness analyzer" should {

    "compute correct metrics" in withJdbc { connection =>

      val tableMissing = getTableMissing(connection)
      val tableMissingColumn = getTableMissingColumn(connection)

      assert(Completeness("att1").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.5)))
      assert(Completeness("att2").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att2", Success(0.75)))
      assert(Completeness("att1").calculate(tableMissingColumn) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.0)))
    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val tableEmpty = getTableEmpty(connection)
        val t = Completeness("att1").calculate(tableEmpty)
        assert(t.value.isFailure)
      }

      "fail on wrong column input" in withJdbc { connection =>
        val tableMissing = getTableMissing(connection)

        Completeness("someMissingColumn").calculate(tableMissing) match {
          case metric =>
            assert(metric.entity == Entity.Column)
            assert(metric.name == "Completeness")
            assert(metric.instance == "someMissingColumn")
            assert(metric.value.isFailure)
        }
      }
    }

    "work with filtering" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      val result = Completeness("att1", Some("item IN ('1', '2')")).calculate(tableMissing)
      assert(result == DoubleMetric(Entity.Column, "Completeness", "att1", Success(1.0)))
    }

    "prevent sql injections" should {

      "prevent sql injection in table name for completeness" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val tableWithInjection = Table(
          "s\"${table.name}; DROP TABLE ${table.name};\"", connection)
        assert(Completeness("att1").calculate(tableWithInjection).value.isFailure)

        assert(hasTable(connection, table.name))
      }
      "prevent sql injection in column name for completeness" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val columnWithInjection = s"1 THEN 1 ELSE 0); DROP TABLE ${table.name};"
        assert(Completeness(columnWithInjection).calculate(table).value.isFailure)

        assert(hasTable(connection, table.name))
      }
      "prevent sql injections in where clause for completeness" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        assert(Completeness("att1", Some (s"';DROP TABLE ${table.name};--"))
          .calculate(table).value.isFailure)

        assert(hasTable(connection, table.name))
      }
    }
  }

  "Uniqueness analyzers" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(Uniqueness("att1").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att1", Success(0.0)))
      assert(Uniqueness("att2").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att2", Success(0.0)))


      assert(Uniqueness("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Uniqueness", "att1", Success(0.25)))
      assert(Uniqueness("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Uniqueness", "att2", Success(0.25)))

    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(Uniqueness("att1").calculate(table).value.isFailure)
      }
    }
    "compute correct metrics on multi columns" in withJdbc { connection =>
      val tableFull = getTableWithUniqueColumns(connection)

      assert(Uniqueness("uniqueValues").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueValues", Success(1.0)))
      assert(Uniqueness("uniqueWithNulls").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueWithNulls", Success(5 / 6.0)))

      assert(Uniqueness(Seq("uniqueValues", "nonUniqueValues")).calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "uniqueValues,nonUniqueValues",
          Success(1.0)))
      assert(Uniqueness(Seq("uniqueValues", "nonUniqueWithNulls")).calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "uniqueValues,nonUniqueWithNulls",
          Success(3 / 6.0)))
      assert(Uniqueness(Seq("nonUniqueValues", "onlyUniqueWithOtherNonUnique"))
        .calculate(tableFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness",
          "nonUniqueValues,onlyUniqueWithOtherNonUnique", Success(1.0)))
    }

    "fail on wrong column input" in withJdbc { connection =>
      val tableFull = getTableWithUniqueColumns(connection)

      Uniqueness("nonExistingColumn").calculate(tableFull) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }

      Uniqueness(Seq("nonExistingColumn", "uniqueValues")).calculate(tableFull) match {
        case metric =>
          assert(metric.entity == Entity.Mutlicolumn)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn,uniqueValues")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }
    }

    "prevent sql injections" should {

      "prevent sql injections in table name for uniqueness" in withJdbc { connection =>
        val table = getTableFull(connection)
        val tableWithInjection = Table(
          s"${table.name}) AS num_rows; DROP TABLE ${table.name};", connection)
        assert(Uniqueness("att1").calculate(tableWithInjection).value.isFailure)

        assert(hasTable(connection, table.name))
      }
      "prevent sql injections in column name for uniqueness" in withJdbc { connection =>
        val table = getTableFull(connection)
        val columnWithInjection = s"nonExistingColumnName"
        assert(Uniqueness(columnWithInjection).calculate(table).value.isFailure)

        assert(hasTable(connection, table.name))
      }
    }
  }

  "UniqueValueRatio analyzer" should {

    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(UniqueValueRatio("att1").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "UniqueValueRatio", "att1", Success(0.0)))
      assert(UniqueValueRatio("att2").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "UniqueValueRatio", "att2", Success(0.0)))


      assert(UniqueValueRatio("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
        "UniqueValueRatio", "att1", Success(0.5)))
      assert(UniqueValueRatio("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
        "UniqueValueRatio", "att2", Success(0.5)))
    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(UniqueValueRatio("att1").calculate(table).value.isFailure)
      }
    }
  }

  "Entropy analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableFull = getTableFull(connection)
      val tableDistinct = getTableWithDistinctValues(connection)

      assert(Entropy("att1").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
      assert(Entropy("att2").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))

      assert(Entropy("att1").calculate(tableDistinct) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(
            -((1.0 / 3) * math.log(1.0 / 3)
              + (1.0 / 3) * math.log(1.0 / 3)
              + (1.0 / 6) * math.log(1.0 / 6)))))
      assert(Entropy("att2").calculate(tableDistinct) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.5 * math.log(0.5) + (1.0 / 6) * math.log(1.0 / 6)))))
    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(Entropy("att1").calculate(table).value.isFailure)
      }
      "fail on empty column" in withJdbc { connection =>
        val table = getTableMissingColumn(connection)
        assert(Entropy("att1").calculate(table).value.isFailure)
      }

    }
  }

  "Compliance analyzer" should {

    "compute correct metrics" in withJdbc { connection =>

      val tableNumeric = getTableWithNumericValues(connection)
      val tableMissing = getTableMissing(connection)
      val tableWhitespace = getTableWithWhitespace(connection)

      assert(Compliance("rule1", "att1 > 3").calculate(tableNumeric) ==
        DoubleMetric(Entity.Column, "Compliance", "rule1", Success(3.0 / 6)))
      assert(Compliance("rule2", "att1 > 2").calculate(tableNumeric) ==
        DoubleMetric(Entity.Column, "Compliance", "rule2", Success(4.0 / 6)))
      assert(Compliance("is item specified?", "item IS NOT NULL").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Compliance", "is item specified?", Success(1.0)))

      // test predicate with multiple columns
      assert(Compliance("ratio of complete lines",
        "item IS NOT NULL AND att1 IS NOT NULL AND att2 IS NOT NULL").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Compliance", "ratio of complete lines", Success(4.0 / 12)))

      // test predicate with value range
      assert(Compliance("value range", "att2 IN ('d', 'f')").calculate(tableMissing) ==
        DoubleMetric(
          Entity.Column,
          "Compliance",
          "value range",
          Success(9.0 / 12)))
      assert(
        Compliance(
          "value range with white space",
          "att2 IN ('x', ' ')"
        ).calculate(tableWhitespace) ==
          DoubleMetric(Entity.Column, "Compliance", "value range with white space", Success(1.0)))
      assert(
        Compliance(
          "value range with empty string",
          "att2 IN ('x', '')").calculate(tableWhitespace) ==
          DoubleMetric(Entity.Column, "Compliance", "value range with empty string", Success(0.5)))

      // test predicate with like
      assert(Compliance("value format", "att1 LIKE '_'").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Compliance", "value format", Success(6.0 / 12)))

      // test wrong predicate
      assert(Compliance("shenanigans", "att1 IN ('d', 'f')").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Compliance", "shenanigans", Success(0)))
    }

    "error handling" should {

      "fail on emtpy table" in withJdbc { connection =>
        val tableEmtpy = getTableEmpty(connection)
        assert(Compliance("rule1", "att1 > 0").calculate(tableEmtpy).value.isFailure)
      }

      "fail on invalid predicate" in withJdbc { connection =>
        val tableMissing = getTableMissing(connection)

        Compliance("crazyRule", "att1 IS GREAT").calculate(tableMissing) match {
          case metric =>
            assert(metric.entity == Entity.Column)
            assert(metric.name == "Compliance")
            assert(metric.instance == "crazyRule")
            assert(metric.value.isFailure)
        }
      }

      "fail on wrong column input" in withJdbc { connection =>
        val tableNumeric = getTableWithNumericValues(connection)
        Compliance("rule1", "attNoSuchColumn > 3").calculate(tableNumeric) match {
          case metric =>
            assert(metric.entity == Entity.Column)
            assert(metric.name == "Compliance")
            assert(metric.instance == "rule1")
            assert(metric.value.isFailure)
        }
      }

    }

    "work with filtering" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      val result =
        Compliance("att1 complete for first two items",
          "att1 IS NOT NULL",
          Some("item IN ('1', '2')")).calculate(tableMissing)
      assert(result == DoubleMetric(
        Entity.Column,
        "Compliance",
        "att1 complete for first two items",
        Success(1.0))
      )
    }

    "prevent sql injections" should {

      "prevent sql injection in table name for compliance" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val tableWithInjection = Table(
          s"${table.name}; DROP TABLE ${table.name};", connection)
        assert(Compliance("rule", "TRUE").calculate(tableWithInjection).value.isFailure)
      }
      "prevent sql injection in predicate for compliance" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val predicateWithInjection = s"TRUE THEN 1 ELSE 0 END); DROP TABLE ${table.name}; --"
        assert(
          Compliance("dangerousRule", predicateWithInjection).calculate(table).value.isFailure)
      }

    }
  }

  "Histogram analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableEmpty = getTableEmpty(connection)

      assert(Histogram("att1").calculate(tableEmpty) == HistogramMetric("att1",
        Success(Distribution(Map(), 0))))

      val dfFull = getTableMissing(connection)
      val histogram = Histogram("att1").calculate(dfFull)
      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 3)
          assert(hv.values.keys == Set("a", "b", Histogram.NullFieldReplacement))

      }
    }

    "compute correct metrics after binning if provided" in withJdbc { connection =>
      val customBinner = "WHEN att1 = 'a' OR att1 = 'b' THEN 'Value1' ELSE 'Value2'"
      val dfFull = getTableMissing(connection)
      val histogram = Histogram("att1", Some(customBinner)).calculate(dfFull)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 2)
          assert(hv.values.keys == Set("Value1", "Value2"))

      }
    }

    "compute correct metrics should only get top N bins" in withJdbc { connection =>
      val dfFull = getTableMissing(connection)
      val histogram = Histogram("att1", None, 2).calculate(dfFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 2)
          assert(hv.values.keys == Set("a", Histogram.NullFieldReplacement))

      }
    }

    "fail for max detail bins > 1000" in withJdbc { connection =>
      val df = getTableFull(connection)
      Histogram("att1", maxDetailBins = 1001).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Cannot return " +
          "histogram values for more than 1000 values"
        case _ => fail("test  was expected to fail due to parameter precondition")
      }
    }
  }

  "Basic statistics" should {

    "Mean analyzer" should {
      "compute mean correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Mean("att1").calculate(table).value
        result shouldBe Success(3.5)
      }

      "compute mean correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Mean("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(3.0)
      }

      "error handling for mean" should {
        "fail to compute mean for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(Mean("att1").calculate(table).value.isFailure)
        }
        "fail to compute mean for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(Mean("att1").calculate(table).value.isFailure)
        }
        "fail to compute mean for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(Mean("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for mean" should {
        "prevent sql injections in table name for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(Mean("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(Mean(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(Mean("att1", Some(s"';DROP TABLE ${table.name};--"))
            .calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
    }

    "Standard deviation analyzer" should {
      "compute standard deviation correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = StandardDeviation("att1").calculate(table).value
        result shouldBe Success(1.707825127659933)
      }

      "compute standard deviation correctly for numeric data with filtering" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val result = StandardDeviation("att1", where =
            Some("item != '6'")).calculate(table).value
          result shouldBe Success(1.4142135623730951)
      }

      "error handling for standard deviation" should {
        "fail to compute standard deviation for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(StandardDeviation("att1").calculate(table).value.isFailure)
        }
        "fail to compute standard deviation for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(StandardDeviation("att1").calculate(table).value.isFailure)
        }
        "fail to compute standard deviaton for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(StandardDeviation("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for standard deviation" should {
        "prevent sql injections in table name for standard deviaton" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(StandardDeviation("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for standard deviaton" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"nonExistingColumnName"
          assert(StandardDeviation(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for standard deviation" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(StandardDeviation("att1", Some (s"';DROP TABLE ${table.name};--"))
            .calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
    }

    "Minimum analyzer" should {
      "compute minimum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Minimum("att1").calculate(table).value
        result shouldBe Success(1.0)
      }
      "compute minimum correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Minimum("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(1.0)
      }

      "error handling for minimum" should {
        "fail to compute minimum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(Minimum("att1").calculate(table).value.isFailure)
        }
        "fail to compute minimum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(Minimum("att1").calculate(table).value.isFailure)
        }
        "fail to compute minimum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(Minimum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for minimum" should {
        "prevent sql injections in table name for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(Minimum("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(Minimum(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(Minimum("att1", Some (s"';DROP TABLE ${table.name};--"))
            .calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
    }

    "Maximum analyzer" should {
      "compute maximum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Maximum("att1").calculate(table).value
        result shouldBe Success(6.0)
      }

      "compute maximum correctly for numeric data with filtering" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val result = Maximum("att1", where = Some("item != '6'")).calculate(table).value
          result shouldBe Success(5.0)
        }

      "error handling for maximum" should {
        "fail to compute maximum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(Maximum("att1").calculate(table).value.isFailure)
        }
        "fail to compute maximum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(Maximum("att1").calculate(table).value.isFailure)
        }
        "fail to compute maximum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(Maximum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for maximum" should {
        "prevent sql injections in table name for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(Maximum("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(Maximum(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(Maximum("att1", Some (s"';DROP TABLE ${table.name};--"))
            .calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
    }

    "Sum analyzer" should {
      "compute sum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        Sum("att1").calculate(table).value shouldBe Success(21)
      }

      "compute sum correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = Sum("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(15.0)
      }

      "should work correctly on decimal columns" in withJdbc { connection =>
        /*
        val schema =
          StructType(StructField(name = "num", dataType = DecimalType.SYSTEM_DEFAULT) :: Nil)

        val rows = session.sparkContext.parallelize(Seq(
          Row(BigDecimal(123.45)),
          Row(BigDecimal(99)),
          Row(BigDecimal(678))))

        val data = session.createDataFrame(rows, schema)

        val result = Minimum("num").calculate(data)

        assert(result.value.isSuccess)
        assert(result.value.get == 99.0)
        */
      }

      "error handling for sum" should {
        "fail to compute sum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(Sum("att1").calculate(table).value.isFailure)
        }
        "fail to compute sum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(Sum("att1").calculate(table).value.isFailure)
        }
        "fail to compute sum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(Sum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for sum" should {
        "prevent sql injections in table name for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(Sum("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(Sum(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(Sum("att1", Some (s"';DROP TABLE ${table.name};--"))
            .calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
    }
  }

  "Count distinct analyzers" should {
    // TODO
    "compute approximate distinct count for numeric data" in withJdbc { connection =>
      /*
      val table = getTableWithUniqueColumns(connection)
      val result = ApproxCountDistinct("uniqueWithNulls").calculate(table).value

      result shouldBe Success(5.0)
      */
    }

    "compute approximate distinct count for numeric data with filtering" in
      {

        /*
        val table = getTableWithUniqueColumns(connection)
        val result = ApproxCountDistinct("uniqueWithNulls", where = Some("unique < 4"))
          .calculate(table).value
        result shouldBe Success(2.0)
        */
      }

      "compute correct metrics for distinct count" should {
        "compute exact distinct count of elements for numeric data" in withJdbc {
          connection =>
            val table = getTableWithUniqueColumns(connection)
            val result = CountDistinct("uniqueWithNulls").calculate(table).value
            result shouldBe Success(5.0)
        }
        "compute exact distinct count of elements for empty table" in withJdbc {
          connection =>
            val table = getTableEmpty(connection)
            val result = CountDistinct("att1").calculate(table).value
            result shouldBe Success(0.0)
        }
        "compute exact distinct count of elements for empty column" in withJdbc {
          connection =>
            val table = getTableMissingColumn(connection)
            val result = CountDistinct("att1").calculate(table).value
            result shouldBe Success(0.0)
        }
      }

      "prevent sql injection for distinct count" should {
        "prevent sql injections in table name for count distinct" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(
            s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(CountDistinct("att1").calculate(tableWithInjection).value.isFailure)

          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for count distinct" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(CountDistinct(columnWithInjection).calculate(table).value.isFailure)

          assert(hasTable(connection, table.name))
        }
      }
  }
/*
  "Correlation analyzer" should {
    "yield NaN for conditionally uninformative columns" in withJdbc { connection =>
      val table = getTableWithConditionallyUninformativeColumns(connection)
      val corr = Correlation("att1", "att2").calculate(table).value.get
      assert(java.lang.Double.isNaN(corr))
    }
    "yield 1.0 for maximal conditionally informative columns" in withJdbc { connection =>
      val table = getTableWithConditionallyInformativeColumns(connection)
      val result = Correlation("att1", "att2").calculate(table)
      result shouldBe DoubleMetric(
        Entity.Mutlicolumn,
        "Correlation",
        "att1,att2",
        Success(1.0)
      )
    }
    "be commutative" in withJdbc { connection =>
      val table = getTableWithConditionallyInformativeColumns(connection)
      Correlation("att1", "att2").calculate(table).value shouldBe
        Correlation("att2", "att1").calculate(table).value
    }
    "work with filtering" in withJdbc { connection =>
      val table = getTableWithNumericFractionalValues(connection)
      val result = Correlation("att1", "att2", Some("att2 > 0")).calculate(table)
      result shouldBe DoubleMetric(
        Entity.Mutlicolumn,
        "Correlation",
        "att1,att2",
        Success(1.0)
      )
    }
    "yield -1.0 for inversed columns" in withJdbc { connection =>
      val table = getTableWithInverseNumberedColumns(connection)
      val result = Correlation("att1", "att2").calculate(table)
      result shouldBe DoubleMetric(
        Entity.Mutlicolumn,
        "Correlation",
        "att1,att2",
        Success(-1.0)
      )
    }
    "yield 0.5 for partly correlated columns" in withJdbc { connection =>
      val table = getTableWithPartlyCorrelatedColumns(connection)
      val result = Correlation("att1", "att2").calculate(table)
      result shouldBe DoubleMetric(
        Entity.Mutlicolumn,
        "Correlation",
        "att1,att2",
        Success(0.5)
      )
    }
  }
*/
  "Distinctness analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(Distinctness("att1").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Distinctness", "att1", Success(1.0 / 6)))
      assert(Distinctness("att2").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Distinctness", "att2", Success(1.0 / 6)))

      assert(Distinctness("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Distinctness", "att1", Success(0.5)))
      assert(Distinctness("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Distinctness", "att2", Success(0.5)))

    }

    "error handling" should {
      "fail on empty column" in withJdbc { connection =>
        val table = getTableMissingColumn(connection)
        assert(Distinctness("att1").calculate(table).value.isFailure)
      }
      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(Distinctness("att1").calculate(table).value.isFailure)
      }
    }
  }
}

object JdbcAnalyzerTests {
  val expectedPreconditionViolation: String =
    "computation was unexpectedly successful, should have failed due to violated precondition"
}
