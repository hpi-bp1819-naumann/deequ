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
package checks

import java.sql.Connection

import com.amazon.deequ.anomalydetection.{Anomaly, AnomalyDetectionStrategy}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.{InMemoryMetricsRepository, MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.jdbc.JdbcHelpers._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{JdbcDataset, JdbcEngine}
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.FixtureSupport
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class CheckTest extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport
  with MockFactory {

  import CheckTest._

  "Check" should {

    "return the correct check status for completeness" in withJdbc { connection =>

      val check1 = Check(CheckLevel.Error, "group-1")
        .isComplete("att1") // 1.0
        .hasCompleteness("att1", _ == 1.0) // 1.0

      val check2 = Check(CheckLevel.Error, "group-2-E")
        .hasCompleteness("att2", _ > 0.8) // 0.75

      val check3 = Check(CheckLevel.Warning, "group-2-W")
        .hasCompleteness("att2", _ > 0.8) // 0.75

      val context = runChecks(getTableCompleteAndInCompleteColumns(connection),
        check1, check2, check3)

      context.metricMap.foreach { println }

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Warning)
    }

    "return the correct check status for uniqueness" in withJdbc { connection =>

      val check = Check(CheckLevel.Error, "group-1")
        .isUnique("uniqueValues")
        .isUnique("uniqueWithNulls")
        .isUnique("nonUniqueValues")
        .isUnique("nonUniqueWithNulls")

      val context = runChecks(getTableWithUniqueColumns(connection), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map(_.status)
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Failure)
      assert(constraintStatuses(2) == ConstraintStatus.Failure)
      assert(constraintStatuses(3) == ConstraintStatus.Failure)
    }

    "return the correct check status for distinctness" in withJdbc { connection =>

      val check = Check(CheckLevel.Error, "distinctness-check")
        .hasDistinctness(Seq("att1"), _ == 0.5)
        .hasDistinctness(Seq("att1", "att2"), _ == 1.0 / 3)
        .hasDistinctness(Seq("att2"), _ == 1.0)

      val context = runChecks(getTableWithDistinctValues(connection), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map { _.status }
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Failure)
    }

    "return the correct check status for hasUniqueness" in withJdbc { connection =>

      val check = Check(CheckLevel.Error, "group-1-u")
        .hasUniqueness("nonUniqueValues", (fraction: Double) => fraction == .5)
        .hasUniqueness("nonUniqueValues", (fraction: Double) => fraction < .6)
        .hasUniqueness(Seq("halfUniqueCombinedWithNonUnique", "nonUniqueValues"),
          (fraction: Double) => fraction == .5)
        .hasUniqueness(Seq("onlyUniqueWithOtherNonUnique", "nonUniqueValues"), Check.IsOne)
        .hasUniqueness("uniqueValues", Check.IsOne)
        .hasUniqueness("uniqueWithNulls", Check.IsOne)

      val context = runChecks(getTableWithUniqueColumns(connection), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map { _.status }
      // Half of nonUnique column are duplicates
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      // Half of the 2 columns are duplicates as well.
      assert(constraintStatuses(2) == ConstraintStatus.Success)
      // Both next 2 cases are actually unique so should meet threshold
      assert(constraintStatuses(3) == ConstraintStatus.Success)
      assert(constraintStatuses(4) == ConstraintStatus.Success)
      // Nulls are duplicated so this will not be unique
      assert(constraintStatuses(5) == ConstraintStatus.Failure)
    }

    "return the correct check status for size" in withJdbc { connection =>
      val (table, numberOfSeqs) = getTableCompleteAndInCompleteColumnsWithSize(connection)

      val check1 = Check(CheckLevel.Error, "group-1-S-1")
        .hasSize(_ == numberOfSeqs)

      val check2 = Check(CheckLevel.Warning, "group-1-S-2")
        .hasSize(_ == numberOfSeqs)

      val check3 = Check(CheckLevel.Error, "group-1-E")
        .hasSize(_ != numberOfSeqs)

      val check4 = Check(CheckLevel.Warning, "group-1-W")
        .hasSize(_ != numberOfSeqs)

      val check5 = Check(CheckLevel.Warning, "group-1-W-Range")
        .hasSize { size => size > 0 && size < numberOfSeqs + 1 }

      val context = runChecks(table, check1, check2, check3, check4, check5)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Success)
      assertEvaluatesTo(check3, context, CheckStatus.Error)
      assertEvaluatesTo(check4, context, CheckStatus.Warning)
      assertEvaluatesTo(check5, context, CheckStatus.Success)
    }

    "return the correct check status for columns constraints" in withJdbc { connection =>

      val check1 = Check(CheckLevel.Error, "group-1")
        .satisfies("att1 > 0", "rule1")

      val check2 = Check(CheckLevel.Error, "group-2-to-fail")
        .satisfies("att1 > 3", "rule2")

      val check3 = Check(CheckLevel.Error, "group-2-to-succeed")
        .satisfies("att1 > 3", "rule3", _ == 0.5)

      val context = runChecks(getTableWithNumericValues(connection), check1, check2, check3)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Success)
    }

    "return the correct check status for conditional column constraints" in
      withJdbc { connection =>

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .satisfies("att1 < att2", "rule1").where("att1 > 3")

        val checkToFail = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule2").where("att1 > 0")

        val checkPartiallyGetsSatisfied = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule3", _ == 0.5).where("att1 > 0")

        val context = runChecks(getTableWithNumericValues(connection), checkToSucceed, checkToFail,
          checkPartiallyGetsSatisfied)

        assertEvaluatesTo(checkToSucceed, context, CheckStatus.Success)
        assertEvaluatesTo(checkToFail, context, CheckStatus.Error)
        assertEvaluatesTo(checkPartiallyGetsSatisfied, context, CheckStatus.Success)
      }

    "correctly evaluate convenience constraints" in withJdbc { connection =>

      val lessThanCheck = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2").where("item > 3")

      val incorrectLessThanCheck = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2")

      val nonNegativeCheck = Check(CheckLevel.Error, "a")
        .isNonNegative("item")

      val isPositiveCheck = Check(CheckLevel.Error, "a")
        .isPositive("item")

      val results = runChecks(getTableWithNumericValues(connection), lessThanCheck,
        incorrectLessThanCheck, nonNegativeCheck, isPositiveCheck)

      assertEvaluatesTo(lessThanCheck, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectLessThanCheck, results, CheckStatus.Error)
      assertEvaluatesTo(nonNegativeCheck, results, CheckStatus.Success)
      assertEvaluatesTo(isPositiveCheck, results, CheckStatus.Success)

      val rangeCheck = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a", "b", "c"))

      val inCorrectRangeCheck = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a", "b"))

      val inCorrectRangeCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a"), _ == 0.5)

      val rangeResults = runChecks(getTableWithDistinctValues(connection), rangeCheck,
        inCorrectRangeCheck, inCorrectRangeCheckWithCustomAssertionFunction)

      assertEvaluatesTo(rangeCheck, rangeResults, CheckStatus.Success)
      assertEvaluatesTo(inCorrectRangeCheck, rangeResults, CheckStatus.Error)
      assertEvaluatesTo(inCorrectRangeCheckWithCustomAssertionFunction, rangeResults,
        CheckStatus.Success)

      val numericRangeCheck1 = Check(CheckLevel.Error, "nr1")
        .isContainedIn("att2", 0, 7)

      val numericRangeCheck2 = Check(CheckLevel.Error, "nr2")
        .isContainedIn("att2", 1, 7)

      val numericRangeCheck3 = Check(CheckLevel.Error, "nr3")
        .isContainedIn("att2", 0, 6)

      val numericRangeCheck4 = Check(CheckLevel.Error, "nr4")
        .isContainedIn("att2", 0, 7, includeLowerBound = false, includeUpperBound = false)

      val numericRangeResults = runChecks(getTableWithNumericValues(connection), numericRangeCheck1,
        numericRangeCheck2, numericRangeCheck3, numericRangeCheck4)

      assertEvaluatesTo(numericRangeCheck1, numericRangeResults, CheckStatus.Success)
      assertEvaluatesTo(numericRangeCheck2, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck3, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck4, numericRangeResults, CheckStatus.Error)
    }

    "return the correct check status for histogram constraints" in
      withJdbc { connection =>

        val check1 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("att1", _ < 10)
          .hasHistogramValues("att1", _ ("a").absolute == 4)
          .hasHistogramValues("att1", _ ("b").absolute == 2)
          .hasHistogramValues("att1", _ ("a").ratio > 0.6)
          .hasHistogramValues("att1", _ ("b").ratio < 0.4)

        val check2 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("att2", _ == 2) //TODO changed this because count distinct does not count null values
          .hasHistogramValues("att2", _ ("f").absolute == 3)
          .hasHistogramValues("att2", _ ("d").absolute == 1)
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).absolute == 2)
          .hasHistogramValues("att2", _ ("f").ratio == 3 / 6.0)
          .hasHistogramValues("att2", _ ("d").ratio == 1 / 6.0)
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).ratio == 2 / 6.0)

        val check3 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("unKnownColumn", _ == 3)

        val context = runChecks(getTableCompleteAndInCompleteColumns(connection), check1, check2, check3)

        assertEvaluatesTo(check1, context, CheckStatus.Success)
        assertEvaluatesTo(check2, context, CheckStatus.Success)
        assertEvaluatesTo(check3, context, CheckStatus.Error)
      }

    "return the correct check status for entropy constraints" in withJdbc { connection =>

      val expectedValue = -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))

      val check1 = Check(CheckLevel.Error, "group-1")
        .hasEntropy("att1", _ == expectedValue)

      val check2 = Check(CheckLevel.Error, "group-1")
        .hasEntropy("att1", _ != expectedValue)

      val context = runChecks(getTableFull(connection), check1, check2)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
    }

    "yield correct results for basic stats" in withJdbc { connection =>
      val baseCheck = Check(CheckLevel.Error, description = "a description")
      val dfNumeric = getTableWithNumericValues(connection)
      val dfInformative = getTableWithConditionallyInformativeColumns(connection)
      val dfUninformative = getTableWithConditionallyUninformativeColumns(connection)

      val engine = JdbcEngine(connection)

      val numericAnalysis = Seq(
        Minimum("att1"), Maximum("att1"), Mean("att1"), Sum("att1"),
        StandardDeviation("att1")/* TODO approx, ApproxCountDistinct("att1"),
        ApproxQuantile("att1", quantile = 0.5)*/)

      val contextNumeric = engine.compute(JdbcDataset(dfNumeric), numericAnalysis)

      assertSuccess(baseCheck.hasMin("att1", _ == 1.0), contextNumeric)
      assertSuccess(baseCheck.hasMax("att1", _ == 6.0), contextNumeric)
      assertSuccess(baseCheck.hasMean("att1", _ == 3.5), contextNumeric)
      assertSuccess(baseCheck.hasSum("att1", _ == 21.0), contextNumeric)
      assertSuccess(baseCheck.hasStandardDeviation("att1", _ == 1.707825127659933), contextNumeric)
      //TODO approx assertSuccess(baseCheck.hasApproxCountDistinct("att1", _ == 6.0), contextNumeric)
      //TODO approx assertSuccess(baseCheck.hasApproxQuantile("att1", quantile = 0.5, _ == 3.0), contextNumeric)

      val correlationAnalysis = Seq(Correlation("att1", "att2"))

      val contextInformative = engine.compute(JdbcDataset(dfInformative), correlationAnalysis)
      val contextUninformative = engine.compute(JdbcDataset(dfUninformative), correlationAnalysis)

      assertSuccess(baseCheck.hasCorrelation("att1", "att2", _ == 1.0), contextInformative)
      assertSuccess(baseCheck.hasCorrelation("att1", "att2", java.lang.Double.isNaN),
        contextUninformative)
    }

    "work on regular expression patterns for E-Mails" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection, Seq("someone@somewhere.org"),
        Seq("someone@else.com"))
      val check = Check(CheckLevel.Error, "some description")
        .hasPattern(col, Patterns.EMAIL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "fail on mixed data for E-Mail pattern with default assertion" in withJdbc { session =>
      val col = "some"
      val df = tableWithColumn(col, StringType, session, Seq("someone@somewhere.org"),
        Seq("someone@else"))
      val check = Check(CheckLevel.Error, "some description")
        .hasPattern(col, Patterns.EMAIL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "work on regular expression patterns for URLs" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("https://www.example.com/foo/?bar=baz&inga=42&quux"), Seq("https://foo.bar/baz"))
      val check = Check(CheckLevel.Error, "some description").hasPattern(col, Patterns.URL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "fail on mixed data for URL pattern with default assertion" in withJdbc {
      connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("https://www.example.com/foo/?bar=baz&inga=42&quux"), Seq("http:// shouldfail.com"))
      val check = Check(CheckLevel.Error, "some description").hasPattern(col, Patterns.URL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "isCreditCard" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection, Seq("4111 1111 1111 1111"),
        Seq("9999888877776666"))
      val check = Check(CheckLevel.Error, "some description")
        .containsCreditCardNumber(col, _ == 0.5)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "define is E-Mail" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection, Seq("someone@somewhere.org"),
        Seq("someone@else"))
      val check = Check(CheckLevel.Error, "some description").containsEmail(col, _ == 0.5)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "define is US social security number" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection, Seq("111-05-1130"),
        Seq("something else"))
      val check = Check(CheckLevel.Error, "some description")
        .containsSocialSecurityNumber(col, _ == 0.5)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "define is URL" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("https://www.example.com/foo/?bar=baz&inga=42&quux"), Seq("http:// shouldfail.com"))
      val check = Check(CheckLevel.Error, "some description")
        .containsURL(col, _ == 0.5)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "define has data type" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection, Seq("2"), Seq("1.0"))
      val check = Check(CheckLevel.Error, "some description")
        .hasDataType(col, ConstrainableDataTypes.Integral, _ == 0.5)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find credit card numbers embedded in text" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("My credit card number is: 4111-1111-1111-1111."))
      val check = Check(CheckLevel.Error, "some description")
        .containsCreditCardNumber(col, _ == 1.0)
      val context = runChecks(df, check)
      context.allMetrics.foreach(println)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find E-mails embedded in text" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("Please contact me at someone@somewhere.org, thank you."))
      val check = Check(CheckLevel.Error, "some description").containsEmail(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find URLs embedded in text" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("Hey, please have a look at https://www.example.com/foo/?bar=baz&inga=42&quux!"))
      val check = Check(CheckLevel.Error, "some description").containsURL(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find SSNs embedded in text" in withJdbc { connection =>
      val col = "some"
      val df = tableWithColumn(col, StringType, connection,
        Seq("My SSN is 111-05-1130, thanks."))
      val check = Check(CheckLevel.Error, "some description")
        .containsSocialSecurityNumber(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "non negativity check works for numeric columns" in withJdbc { connection =>
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
        assertNonNegativeCheckIsSuccessFor(dataType, connection)
      }
    }

    "is positive check works for numeric columns" in withJdbc { connection =>
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
        assertIsPositiveCheckIsSuccessFor(dataType, connection)
      }
    }
  }

  "Check isNewestPointNonAnomalous" should {

    "return the correct check status for anomaly detection for different analyzers" in
      withJdbc { connection =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 3.0, 4.0, 11.0), (4, 5))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 3.0, 4.0, 4.0), (4, 5))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
            // Distinctness results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 3.0, 4.0, 1), (4, 5))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 3.0, 4.0, 1), (4, 5))
              .returns(Seq((4, Anomaly(Option(4.0), 0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Seq(Size(), Distinctness(Seq("c0", "c1")))

          val context11Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 11), analysis)
          val context4Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 4), analysis)
          val contextNoSeqs = JdbcEngine.computeOn(getTableEmpty(connection), analysis)

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(), Map.empty,
              None, None)

          assert(sizeAnomalyCheck.evaluate(context11Seqs).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Seqs).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoSeqs).status == CheckStatus.Error)

          // Now with Distinctness
          val distinctnessAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector,
              Distinctness(Seq("c0", "c1")), Map.empty, None, None)

          assert(distinctnessAnomalyCheck.evaluate(context11Seqs).status == CheckStatus.Success)
          assert(distinctnessAnomalyCheck.evaluate(context4Seqs).status == CheckStatus.Error)
          assert(distinctnessAnomalyCheck.evaluate(contextNoSeqs).status == CheckStatus.Error)
        }
      }

     "only use historic results filtered by tagValues if specified" in
      withJdbc { connection =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Seq(Size())

          val context11Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 11), analysis)
          val context4Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 4), analysis)
          val contextNoSeqs = JdbcEngine.computeOn(getTableEmpty(connection), analysis)

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map("Region" -> "EU"), None, None)

          assert(sizeAnomalyCheck.evaluate(context11Seqs).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Seqs).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoSeqs).status == CheckStatus.Error)
        }
      }

    "only use historic results after some dateTime if specified" in
      withJdbc { connection =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(3.0, 4.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(3.0, 4.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Seq(Size())

          val context11Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 11), analysis)
          val context4Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 4), analysis)
          val contextNoSeqs = JdbcEngine.computeOn(getTableEmpty(connection), analysis)

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map.empty, Some(3), None)

          assert(sizeAnomalyCheck.evaluate(context11Seqs).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Seqs).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoSeqs).status == CheckStatus.Error)
        }
      }

    "only use historic results before some dateTime if specified" in
      withJdbc { connection =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Seq(Size())

          val context11Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 11), analysis)
          val context4Seqs = JdbcEngine.computeOn(getTableWithNRows(connection, 4), analysis)
          val contextNoSeqs = JdbcEngine.computeOn(getTableEmpty(connection), analysis)

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map.empty, None, Some(2))

          assert(sizeAnomalyCheck.evaluate(context11Seqs).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Seqs).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoSeqs).status == CheckStatus.Error)
        }
      }
  }

  /** Run anomaly detection using a repository with some previous analysis results for testing */
  private[this] def evaluateWithRepository(test: MetricsRepository => Unit): Unit = {

    val repository = createRepository()

    (1 to 2).foreach { timeStamp =>
      val analyzerContext = ComputedStatistics(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp)),
        Distinctness(Seq("c0", "c1")) -> DoubleMetric(Entity.Column, "", "",
          Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "EU")), analyzerContext)
    }

    (3 to 4).foreach { timeStamp =>
      val analyzerContext = ComputedStatistics(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp)),
        Distinctness(Seq("c0", "c1")) -> DoubleMetric(Entity.Column, "", "",
          Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "NA")), analyzerContext)
    }
    test(repository)
  }

   /** Create a repository for testing */
  private[this] def createRepository(): MetricsRepository = {
    new InMemoryMetricsRepository()
  }
}

object CheckTest extends WordSpec with Matchers {

  def assertSuccess(check: Check, context: ComputedStatistics): Unit = {
    check.evaluate(context).status shouldBe CheckStatus.Success
  }

  def assertEvaluatesTo(
    check: Check,
    context: ComputedStatistics,
    status: CheckStatus.Value)
  : Unit = {

    assert(check.evaluate(context).status == status)
  }

  def runChecks(data: Table, check: Check, checks: Check*): ComputedStatistics = {
    val analyzers = (check.requiredAnalyzers() ++ checks.flatMap { _.requiredAnalyzers() }).toSeq

    val engine = JdbcEngine(data.jdbcConnection)

    engine.compute(JdbcDataset(data), analyzers)
  }

  private[this] def runAndAssertSuccessFor[T](
    checkOn: String => Check, dataType: JdbcNumericDataType, connection: Connection
  ): Unit = {

    val col = "some"
    val numericSeq = dataType match {
      case FloatType => Seq(1.0f)
      case DoubleType => Seq(1.0d)
      case ByteType => Seq(1.toByte)
      case ShortType => Seq(1.toShort)
      case IntegerType => Seq(1)
      case LongType => Seq(1L)
    }
    val df = tableWithColumn(col, dataType, connection, numericSeq, Seq(null))
    val check = checkOn(col)
    val context = runChecks(df, check)
    assertEvaluatesTo(check, context, CheckStatus.Success)
  }

  def assertNonNegativeCheckIsSuccessFor(
    dataType: JdbcNumericDataType,
    connection: Connection)
  : Unit = {

    runAndAssertSuccessFor(Check(CheckLevel.Error, "some description").isNonNegative(_),
      dataType, connection)
  }

  def assertIsPositiveCheckIsSuccessFor(
    dataType: JdbcNumericDataType,
    connection: Connection)
  : Unit = {

    runAndAssertSuccessFor(Check(CheckLevel.Error, "some description").isPositive(_),
      dataType, connection)
  }
}
