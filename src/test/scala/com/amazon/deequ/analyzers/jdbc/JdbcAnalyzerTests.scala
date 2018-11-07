package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class JdbcAnalyzerTests
  extends WordSpec with Matchers with JdbcContextSpec with JdbcFixtureSupport {

  "Size analyzer" should {
    "compute correct metrics" in withJdbc { connection =>

      val tableMissing = getTableMissingWithSize(connection)
      val tableFull = getTableFullWithSize(connection)

      assert(JdbcSize().calculate(tableMissing._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableMissing._2)))
      assert(JdbcSize().calculate(tableFull._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableFull._2)))

    }
    "prevent sql injections in table name for mean" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcMean("att1").calculate(tableWithInjection).value.isFailure)
    }
  }

  "Completeness analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      /*
      val tableMissing = getTableMissing(connection)

      assert(JdbcCompleteness("someMissingColumn").preconditions.size == 1,
        "should check column name availability")
      assert(JdbcCompleteness("att1").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.5)))
      assert(JdbcCompleteness("att2").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att2", Success(0.75)))
        */

    }
    "fail on wrong column input" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)

      JdbcCompleteness("someMissingColumn").calculate(tableMissing) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Completeness")
          assert(metric.instance == "someMissingColumn")
          assert(metric.value.isFailure)
      }
    }

    "work with filtering" in withJdbc { connection =>
      /*
      val dfMissing = getTableMissing(connection)

      val result = JdbcCompleteness("att1", Some("item IN ('1', '2')")).calculate(dfMissing)
      assert(result == DoubleMetric(Entity.Column, "Completeness", "att1", Success(1.0)))
      */
    }

    "prevent sql injections in table name for completeness" in withJdbc { connection =>
      val table = getTableMissing(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcCompleteness("att1").calculate(tableWithInjection).value.isFailure)
    }

    "prevent sql injections in column name for completeness" in withJdbc { connection =>
      val table = getTableMissing(connection)
      val columnWithInjection = s"1 THEN 1 ELSE 0); DROP TABLE ${table.name};"
      assert(JdbcCompleteness(columnWithInjection).calculate(table).value.isFailure)
    }
  }

  "Uniqueness analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val dfMissing = getTableMissing(connection)
      val dfFull = getTableFull(connection)

      assert(JdbcUniqueness("att1").calculate(dfMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att1", Success(0.0)))
      assert(JdbcUniqueness("att2").calculate(dfMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att2", Success(0.0)))


      assert(JdbcUniqueness("att1").calculate(dfFull) == DoubleMetric(Entity.Column, "Uniqueness",
        "att1", Success(0.25)))
      assert(JdbcUniqueness("att2").calculate(dfFull) == DoubleMetric(Entity.Column, "Uniqueness",
        "att2", Success(0.25)))

    }
    "compute correct metrics on multi columns" in withJdbc { connection =>
      /*
      val dfFull = getTableWithUniqueColumns(connection)

      assert(JdbcUniqueness("unique").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "unique", Success(1.0)))
      assert(JdbcUniqueness("uniqueWithNulls").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueWithNulls", Success(5 / 6.0)))
      assert(JdbcUniqueness(Seq("unique", "nonUnique")).calculate(dfFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "unique,nonUnique", Success(1.0)))
      assert(JdbcUniqueness(Seq("unique", "nonUniqueWithNulls")).calculate(dfFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "unique,nonUniqueWithNulls",
          Success(3 / 6.0)))
      assert(JdbcUniqueness(Seq("nonUnique", "onlyUniqueWithOtherNonUnique")).calculate(dfFull) ==
        DoubleMetric(Entity.Mutlicolumn, "Uniqueness", "nonUnique,onlyUniqueWithOtherNonUnique",
          Success(1.0)))
      */

    }
    "fail on wrong column input" in withJdbc { connection =>
      /*
      val dfFull = getTableWithUniqueColumns(connection)

      JdbcUniqueness("nonExistingColumn").calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }

      JdbcUniqueness(Seq("nonExistingColumn", "unique")).calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Mutlicolumn)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn,unique")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }
      */
    }
    "prevent sql injections in table name for uniqueness" in withJdbc { connection =>
      val table = getTableFull(connection)
      val tableWithInjection =
        Table(s"${table.name}) AS num_rows; DROP TABLE ${table.name};", connection)
      assert(JdbcUniqueness("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for uniqueness" in withJdbc { connection =>
      val table = getTableFull(connection)
      val columnWithInjection = s"nonExistingColumnName"
      assert(JdbcUniqueness(columnWithInjection).calculate(table).value.isFailure)
    }
  }

  "Basic statistics" should {
    "compute mean correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = JdbcMean("att1").calculate(table).value
      result shouldBe Success(3.5)
    }
    "fail to compute mean for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(JdbcMean("att1").calculate(table).value.isFailure)
    }
    "prevent sql injections in table name for mean" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcMean("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for mean" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1); DROP TABLE ${table.name};"
      assert(JdbcMean(columnWithInjection).calculate(table).value.isFailure)
    }

    "compute standard deviation correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = JdbcStandardDeviation("att1").calculate(table).value
      result shouldBe Success(1.707825127659933)
    }
    "fail to compute standard deviaton for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(JdbcStandardDeviation("att1").calculate(table).value.isFailure)
    }
    "prevent sql injections in table name for standard deviaton" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcMean("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for standard deviaton" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"nonExistingColumnName"
      assert(JdbcMean(columnWithInjection).calculate(table).value.isFailure)
    }

    "compute minimum correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = JdbcMinimum("att1").calculate(table).value
      result shouldBe Success(1.0)
    }
    "fail to compute minimum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(JdbcMinimum("att1").calculate(table).value.isFailure)
    }
    "prevent sql injections in table name for minimum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcMinimum("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for minimum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1); DROP TABLE ${table.name};"
      assert(JdbcMinimum(columnWithInjection).calculate(table).value.isFailure)
    }

    "compute maximum correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val result = JdbcMaximum("att1").calculate(table).value
      result shouldBe Success(6.0)
    }
    "compute maximum correctly for numeric data with filtering" in
      withJdbc { connection =>
        /*
        val table = getTableWithNumericValues(connection)
        val result = JdbcMaximum("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(5.0)
        */
      }
    "fail to compute maximum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(JdbcMaximum("att1").calculate(table).value.isFailure)
    }
    "prevent sql injections in table name for maximum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcMaximum("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for maximum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1); DROP TABLE ${table.name};"
      assert(JdbcMaximum(columnWithInjection).calculate(table).value.isFailure)
    }

    "compute sum correctly for numeric data" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      JdbcSum("att1").calculate(table).value shouldBe Success(21)
    }
    "fail to compute sum for non numeric type" in withJdbc { connection =>
      val table = getTableFull(connection)
      assert(JdbcSum("att1").calculate(table).value.isFailure)
    }
    "prevent sql injections in table name for sum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcSum("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for sum" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1); DROP TABLE ${table.name};"
      assert(JdbcSum(columnWithInjection).calculate(table).value.isFailure)
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
  }

  "Count distinct analyzers" should {
    "compute approximate distinct count for numeric data" in withJdbc { connection =>
      /*
      val table = getTableWithUniqueColumns(connection)
      val result = JdbcApproxCountDistinct("uniqueWithNulls").calculate(table).value

      result shouldBe Success(5.0)
      */
    }

    "compute approximate distinct count for numeric data with filtering" in
      withJdbc { connection =>

        /*
        val table = getTableWithUniqueColumns(connection)
        val result = JdbcApproxCountDistinct("uniqueWithNulls", where = Some("unique < 4"))
          .calculate(table).value
        result shouldBe Success(2.0)
        */
      }

    "compute exact distinct count of elements for numeric data" in withJdbc {
      connection =>
        val table = getTableWithUniqueColumns(connection)
        val result = JdbcCountDistinct("uniqueWithNulls").calculate(table).value
        result shouldBe Success(5.0)
    }
    "prevent sql injections in table name for count distinct" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
      assert(JdbcCountDistinct("att1").calculate(tableWithInjection).value.isFailure)
    }
    "prevent sql injections in column name for count distinct" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1); DROP TABLE ${table.name};"
      assert(JdbcCountDistinct(columnWithInjection).calculate(table).value.isFailure)
    }
  }
}

object JdbcAnalyzerTests {
  val expectedPreconditionViolation: String =
    "computation was unexpectedly successful, should have failed due to violated precondition"
}