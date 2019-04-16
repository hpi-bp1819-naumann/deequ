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

package com.amazon.deequ.checks

import com.amazon.deequ.JdbcContextSpec
import com.amazon.deequ.runtime.jdbc.Applicability
import com.amazon.deequ.runtime.jdbc.operators._
import org.scalatest.WordSpec

class ApplicabilityTest extends WordSpec with JdbcContextSpec {

  private[this] val schema = JdbcStructType(Seq(
    JdbcStructField("stringCol", StringType, nullable = true),
    JdbcStructField("stringCol2", StringType, nullable = true),
    JdbcStructField("byteCol", ByteType, nullable = true),
    JdbcStructField("shortCol", ShortType, nullable = true),
    JdbcStructField("intCol", IntegerType, nullable = true),
    JdbcStructField("intCol2", IntegerType, nullable = true),
    JdbcStructField("longCol", LongType, nullable = true),
    JdbcStructField("floatCol", FloatType, nullable = true),
    JdbcStructField("floatCol2", FloatType, nullable = true),
    JdbcStructField("doubleCol", DoubleType, nullable = true),
    JdbcStructField("doubleCol2", DoubleType, nullable = true),
    JdbcStructField("decimalCol", DecimalType, nullable = true),
    JdbcStructField("decimalCol2", DecimalType, nullable = true),
    JdbcStructField("decimalCol3", NumericType(5, 2), nullable = true),
    JdbcStructField("decimalCol4", NumericType(8, 4), nullable = true),
    JdbcStructField("timestampCol", TimestampType, nullable = true),
    JdbcStructField("timestampCol2", TimestampType, nullable = true),
    JdbcStructField("booleanCol", BooleanType, nullable = true),
    JdbcStructField("booleanCol2", BooleanType, nullable = true))
  )

  "Applicability tests for checks" should {

    "recognize applicable checks as applicable" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val validCheck = Check(CheckLevel.Warning, "")
        .isComplete("stringCol")
        .isNonNegative("floatCol")

      val resultForValidCheck = applicability.isApplicable(validCheck, schema)

      assert(resultForValidCheck.isApplicable)
      assert(resultForValidCheck.failures.isEmpty)
      assert(resultForValidCheck.constraintApplicabilities.size == validCheck.constraints.size)
      resultForValidCheck.constraintApplicabilities.foreach { case (_, applicable) =>
        assert(applicable)
      }
    }

    "detect checks with non existing columns" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val checkWithNonExistingColumn = Check(CheckLevel.Warning, "")
        .isComplete("stringColasd")

      val resultForCheckWithNonExistingColumn =
        applicability.isApplicable(checkWithNonExistingColumn, schema)

      assert(!resultForCheckWithNonExistingColumn.isApplicable)
      assert(resultForCheckWithNonExistingColumn.failures.size == 1)
      assert(resultForCheckWithNonExistingColumn.constraintApplicabilities.size ==
        checkWithNonExistingColumn.constraints.size)
      resultForCheckWithNonExistingColumn.constraintApplicabilities.foreach {
        case (_, applicable) => assert(!applicable)
      }
    }

    "detect checks with invalid sql expressions" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val checkWithInvalidExpression1 = Check(CheckLevel.Warning, "")
        .isNonNegative("")

      val resultForCheckWithInvalidExpression1 =
        applicability.isApplicable(checkWithInvalidExpression1, schema)

      assert(!resultForCheckWithInvalidExpression1.isApplicable)
      assert(resultForCheckWithInvalidExpression1.failures.size == 1)


      val checkWithInvalidExpression2 = Check(CheckLevel.Warning, "")
        .isComplete("booleanCol").where("foo + bar___")

      val resultForCheckWithInvalidExpression2 =
        applicability.isApplicable(checkWithInvalidExpression2, schema)

      assert(!resultForCheckWithInvalidExpression2.isApplicable)
      assert(resultForCheckWithInvalidExpression2.failures.size == 1)
    }

    "report on all constraints of the Check" in withJdbc { connection =>
      val applicability = new Applicability(connection)

      val check = Check(CheckLevel.Error, "")
        .isComplete("stringCol")
        .isUnique("stringCol")

      val result = applicability.isApplicable(check, schema)

      assert(result.constraintApplicabilities.size == check.constraints.size)

      check.constraints.foreach { constraint =>
        assert(result.constraintApplicabilities(constraint))
      }
    }
  }

  "Applicability tests for analyzers" should {

    "recognize applicable analyzers as applicable" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val validAnalyzer = CompletenessOp("stringCol")

      val resultForValidAnalyzer = applicability.isApplicable(Seq(validAnalyzer), schema)

      assert(resultForValidAnalyzer.isApplicable)
      assert(resultForValidAnalyzer.failures.isEmpty)
    }

    "detect analyzers for non existing columns" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val analyzerForNonExistingColumn = CompletenessOp("stringColasd")

      val resultForAnalyzerForNonExistingColumn =
        applicability.isApplicable(Seq(analyzerForNonExistingColumn), schema)

      assert(!resultForAnalyzerForNonExistingColumn.isApplicable)
      assert(resultForAnalyzerForNonExistingColumn.failures.size == 1)
    }

    "detect analyzers with invalid sql expressions" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val analyzerWithInvalidExpression1 = ComplianceOp("", "")

      val resultForAnalyzerWithInvalidExpression1 =
        applicability.isApplicable(Seq(analyzerWithInvalidExpression1), schema)

      assert(!resultForAnalyzerWithInvalidExpression1.isApplicable)
      assert(resultForAnalyzerWithInvalidExpression1.failures.size == 1)


      val analyzerWithInvalidExpression2 = CompletenessOp("booleanCol", Some("foo + bar___"))

      val resultForAnalyzerWithInvalidExpression2 =
        applicability.isApplicable(Seq(analyzerWithInvalidExpression2), schema)

      assert(!resultForAnalyzerWithInvalidExpression2.isApplicable)
      assert(resultForAnalyzerWithInvalidExpression2.failures.size == 1)
    }

    "handles min/max with decimal columns" in withJdbc { connection =>

      val applicability = new Applicability(connection)

      val analyzers = Seq(
        MinimumOp("decimalCol"),
        MaximumOp("decimalCol"),
        MinimumOp("decimalCol2"),
        MaximumOp("decimalCol2"),
        MinimumOp("decimalCol3"),
        MaximumOp("decimalCol3"),
        MinimumOp("decimalCol4"),
        MaximumOp("decimalCol4")
      )


      val resultForValidAnalyzer = applicability.isApplicable(analyzers, schema)

      assert(resultForValidAnalyzer.isApplicable)
      assert(resultForValidAnalyzer.failures.isEmpty)
    }

  }
}
