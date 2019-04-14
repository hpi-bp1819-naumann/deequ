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
package constraints

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstraintUtils.calculate
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class ConstraintsTest extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  "Completeness constraint" should {
    "assert on wrong completeness" in withJdbc { connection =>
      val df = getTableMissing(connection)

      val constraint1 = StatisticConstraint[Double, Double](Completeness("att1"), _ == 0.5)
      assert(calculate(constraint1, df).status == ConstraintStatus.Success)

      val constraint2 = StatisticConstraint[Double, Double](Completeness("att1"), _ != 0.5)
      assert(calculate(constraint2, df).status == ConstraintStatus.Failure)

      val constraint3 = StatisticConstraint[Double, Double](Completeness("att2"), _ == 0.75)
      assert(calculate(constraint3, df).status == ConstraintStatus.Success)

      val constraint4 = StatisticConstraint[Double, Double](Completeness("att2"), _ != 0.75)
      assert(calculate(constraint4, df).status == ConstraintStatus.Failure)
    }
  }

  "Histogram constraints" should {

    "assert on ratios for a column value which does not exist" in withJdbc { connection =>
      val df = getTableMissing(connection)

      val constraint = StatisticConstraint[Distribution, Distribution](
        Histogram("att1"), _("non-existent-column-value").ratio == 3)

      val metric = calculate(constraint, df)

      metric match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.isDefined)
          assert(result.message.get.startsWith(StatisticConstraint.AssertionException))
      }
    }
  }

  /*"Mutual information constraint" should {
    "yield a mutual information of 0 for conditionally uninformative columns" in
      withJdbc { connection =>
        val df = getTableWithConditionallyUninformativeColumns(connection)
        val constraint = StatisticConstraint[Double, Double](MutualInformation(Seq("att1", "att2")), _ == 0)
        calculate(constraint, df).status shouldBe ConstraintStatus.Success
      }
  }*/

  "Basic stats constraints" should {
    /*"assert on approximate quantile" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](ApproxQuantile("att1", quantile = 0.5), _ == 3.0)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }*/
    "assert on minimum" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](Minimum("att1"), _ == 1.0)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    "assert on maximum" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](Maximum("att1"), _ == 6.0)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    "assert on mean" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](Mean("att1"), _ == 3.5)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    "assert on sum" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](Sum("att1"), _ == 21)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    "assert on standard deviation" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](StandardDeviation("att1"), _ == 1.707825127659933)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    /*"assert on approximate count distinct" in withJdbc { connection =>
      val df = getTableWithNumericValues(connection)
      val constraint = StatisticConstraint[Double, Double](ApproxCountDistinct("att1"), _ == 6.0)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }*/
  }

  "Correlation constraint" should {
    "assert maximal correlation" in withJdbc { connection =>
      val df = getTableWithConditionallyInformativeColumns(connection)
      val constraint = StatisticConstraint[Double, Double](Correlation("att1", "att2"), _ == 1.0)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
    "assert no correlation" in withJdbc { connection =>
      val df = getTableWithConditionallyUninformativeColumns(connection)
      val constraint = StatisticConstraint[Double, Double](Correlation("att1", "att2"), java.lang.Double.isNaN)
      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
  }

  "Data type constraint" should {
    val column = "column"

    "assert fractional type for DoubleType column" in withJdbc { connection =>
      val df = tableWithColumn(column, DoubleType, connection, Seq(1.0), Seq(2.0))

      val constraint = Check(CheckLevel.Warning, "test")
        .hasDataType(column, ConstrainableDataTypes.Fractional, Check.IsOne)
        .constraints.head

      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }

    "assert fractional type for StringType column" in withJdbc { connection =>
      val df = tableWithColumn(column, StringType, connection, Seq("1"), Seq("2.0"))

      val constraint = Check(CheckLevel.Warning, "test")
        .hasDataType(column, ConstrainableDataTypes.Fractional, _ == 0.5)
        .constraints.head

      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }

    "assert numeric type as sum over fractional and integral" in withJdbc { connection =>
      val df = tableWithColumn(column, StringType, connection, Seq("1"), Seq("2.0"))

      val constraint = Check(CheckLevel.Warning, "test")
        .hasDataType(column, ConstrainableDataTypes.Numeric, Check.IsOne)
        .constraints.head

      calculate(constraint, df).status shouldBe ConstraintStatus.Success
    }
  }

}