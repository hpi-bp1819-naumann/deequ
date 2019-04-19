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

package com.amazon.deequ.runtime.jdbc

import java.sql.{Connection, Timestamp}

import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.{Constraint, StatisticConstraint}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.operators._

import scala.util.{Failure, Random}

private[deequ] sealed trait ApplicabilityResult {
  def isApplicable: Boolean
  def failures: Seq[(String, Exception)]
}

private[deequ] case class CheckApplicability(
  isApplicable: Boolean,
  failures: Seq[(String, Exception)],
  constraintApplicabilities: Map[Constraint, Boolean]
) extends ApplicabilityResult

private[deequ] case class AnalyzersApplicability(
  isApplicable: Boolean,
  failures: Seq[(String, Exception)]
) extends ApplicabilityResult

private[deequ] object Applicability {

  private[this] val DIGITS = Array("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
  private[this] val NUM_DIGITS = DIGITS.length

  private def shouldBeNull(nullable: Boolean): Boolean = {
    nullable && math.random < 0.01
  }

  def randomBoolean(nullable: Boolean): java.lang.Boolean = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextDouble() > 0.5
    }
  }

  def randomInteger(nullable: Boolean): java.lang.Integer = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextInt()
    }
  }

  def randomFloat(nullable: Boolean): java.lang.Float = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextFloat()
    }
  }

  def randomDouble(nullable: Boolean): java.lang.Double = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextDouble()
    }
  }

  def randomByte(nullable: Boolean): java.lang.Byte = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextInt().toByte
    }
  }

  def randomShort(nullable: Boolean): java.lang.Short = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextInt().toShort
    }
  }

  def randomLong(nullable: Boolean): java.lang.Long = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      Random.nextLong()
    }
  }

  def randomDecimal(nullable: Boolean, precision: Int, scale: Int): java.math.BigDecimal = {
    if (shouldBeNull(nullable)) {
      null
    } else {

      /* Generate a string representation of the numeric value of maximal length */
      val number = new StringBuilder(precision + 1)

      /* First digit should not be zero */
      val firstDigit = Random.nextInt(NUM_DIGITS - 1) + 1
      number.append(firstDigit)

      for (_ <- 1 until precision - scale) {
        number.append(DIGITS(Random.nextInt(NUM_DIGITS)))
      }

      if (scale > 0) {
        number.append(".")

        for (_ <- 0 until scale) {
          number.append(DIGITS(Random.nextInt(NUM_DIGITS)))
        }
      }

      BigDecimal(number.toString()).bigDecimal
    }
  }

  def randomTimestamp(nullable: Boolean): java.sql.Timestamp = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      new Timestamp(Random.nextLong())
    }
  }

  def randomString(nullable: Boolean): java.lang.String = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      val length = util.Random.nextInt(20) + 1
      Random.alphanumeric.take(length).mkString
    }
  }
}

/**
  * A class to Check whether a check is applicable to some data using the schema of the data.
  *
  * @param connection The JDBC connection in order to be able to create fake data
  */
private[deequ] class Applicability(connection: Connection) {

  import Applicability._

  /**
    * Check whether a check is applicable to some data using the schema of the data.
    *
    * @param check A check that may be applicable to some data
    * @param schema The schema of the data the checks are for
    */
  def isApplicable(check: Check, schema: JdbcStructType):
  CheckApplicability = {

    val data = generateRandomData(schema, 1000, connection)

    val engine = JdbcEngine(connection)

    val namedMetrics = check.constraints
      .map { constraint => constraint.toString -> constraint }
      .collect { case (name, constraint: StatisticConstraint[_, _]) =>

        val operator = JdbcEngine.matchingOperator(constraint.statistic)
        val metric = operator.calculate(data).value

        name -> metric
      }

    val constraintApplicabilities = check.constraints.zip(namedMetrics).map {
      case (constraint, (_, metric)) => constraint -> metric.isSuccess
    }
    .toMap

    val failures = namedMetrics
      .flatMap { case (name, metric) =>

        metric match {
          // An exception occurred during analysis
          case Failure(exception: Exception) => Some(name -> exception)
          // Analysis done successfully and result metric is there
          case _ => None
        }
      }


    CheckApplicability(failures.isEmpty, failures, constraintApplicabilities)
  }

  /**
    * Check whether analyzers are applicable to some data using the schema of the data.
    *
    * @param analyzers Analyzers that may be applicable to some data
    * @param schema The schema of the data the analyzers are for
    */
  def isApplicable(
      analyzers: Seq[Operator[_ <: State[_], Metric[_]]],
      schema: JdbcStructType)
    : AnalyzersApplicability = {

    val data = generateRandomData(schema, 1000, connection)

    val analyzersByName = analyzers
      .map { analyzer => analyzer.toString -> analyzer }

    val failures = analyzersByName
      .flatMap { case (name, analyzer) =>
        val maybeValue = analyzer.calculate(data).value

        maybeValue match {
          // An exception occurred during analysis
          case Failure(exception: Exception) => Some(name -> exception)
          // Analysis done successfully and result metric is there
          case _ => None
        }
      }

    AnalyzersApplicability(failures.isEmpty, failures)
  }


  def generateRandomData(schema: JdbcStructType, numRows: Int, connection: Connection): Table = {

    val data = (0 to numRows).map { _ =>

      val cells = schema.fields.map(field => {
        val dataType = field.dataType

        val cell = dataType match {
          case StringType => randomString(false)
          case IntegerType => randomInteger(false)
          case FloatType => randomFloat(false)
          case DoubleType => randomDouble(false)
          case ByteType => randomByte(false)
          case ShortType => randomShort(false)
          case LongType => randomLong(false)
          case DecimalType => randomDecimal(false, 10, 0) //TODO
          case TimestampType => randomTimestamp(false)
          case BooleanType => randomBoolean(false)

          case _ =>
            // TODO: find better solution for this
            if (dataType.isInstanceOf[NumericType]) {
              val decimalType = dataType.asInstanceOf[NumericType]
              randomDecimal(false, decimalType.precision, decimalType.scale)
            }
            else {
              throw new IllegalArgumentException(
                "Applicability check can only handle basic datatypes " +
                  s"for columns (string, integer, float, double, decimal, boolean) " +
                  s"not $dataType")
            }
        }

        cell
      })

      cells.toSeq
    }

    JdbcHelpers.fillTableWithData("randomData", schema, data, connection, temporary = true)
  }
}
