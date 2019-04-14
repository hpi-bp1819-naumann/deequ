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

package com.amazon.deequ.runtime.jdbc.operators

import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.runtime.jdbc.operators.Operators._
import com.amazon.deequ.runtime.jdbc.operators.Preconditions._

case class CorrelationState(
    n: Double,
    xAvg: Double,
    yAvg: Double,
    ck: Double,
    xMk: Double,
    yMk: Double)
  extends DoubleValuedState[CorrelationState] {

  require(n > 0.0, "Correlation undefined for n = 0.")

  override def sum(other: CorrelationState): CorrelationState = {
    val n1 = n
    val n2 = other.n
    val newN = n1 + n2
    val dx = other.xAvg - xAvg
    val dxN = if (newN == 0.0) 0.0 else dx / newN
    val dy = other.yAvg - yAvg
    val dyN = if (newN == 0.0) 0.0 else dy / newN
    val newXAvg = xAvg + dxN * n2
    val newYAvg = yAvg + dyN * n2
    val newCk = ck + other.ck + dx * dyN * n1 * n2
    val newXMk = xMk + other.xMk + dx * dxN * n1 * n2
    val newYMk = yMk + other.yMk + dy * dyN * n1 * n2

    CorrelationState(newN, newXAvg, newYAvg, newCk, newXMk, newYMk)
  }

  override def metricValue(): Double = {
    ck / math.sqrt(xMk * yMk)
  }
}

/**
  * Computes the pearson correlation coefficient between the two given columns
  *
  * @param firstColumn First input column for computation
  * @param secondColumn Second input column for computation
  */
case class CorrelationOp(
    firstColumn: String,
    secondColumn: String,
    where: Option[String] = None)
  extends StandardScanShareableOperator[CorrelationState]("Correlation",
    s"$firstColumn,$secondColumn", Entity.Mutlicolumn) {

  override def aggregationFunctions(): Seq[String] = {

    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$firstColumn * $secondColumn")})" ::
      s"AVG(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$firstColumn")})" ::
      s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$secondColumn")})" ::
      s"AVG(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$secondColumn")})" ::
      s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$firstColumn")})" ::
      s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
        s"1")})" ::
      s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$firstColumn * $firstColumn")})" ::
      s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
        s"$secondColumn * $secondColumn")})" ::
      Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): Option[CorrelationState] = {

    ifNoNullsIn(result, offset, 8) { _ =>
      val numRows = result.getDouble(5)
      val sumFirstTimesSecond = result.getDouble(0)
      val averageFirst = result.getDouble(1)
      val sumSecond = result.getDouble(2)
      val averageSecond = result.getDouble(3)
      val sumFirst = result.getDouble(4)
      val sumFirstSquared = result.getDouble(6)
      val sumSecondSquared = result.getDouble(7)

      val ck = sumFirstTimesSecond - (averageSecond * sumFirst) - (averageFirst * sumSecond) +
        (numRows * averageFirst * averageSecond)
      val xmk = sumFirstSquared - (averageFirst * sumFirst) - (averageFirst * sumFirst) +
        (numRows * averageFirst * averageFirst)
      val ymk = sumSecondSquared - 2 * averageSecond * sumSecond +
        (numRows * averageSecond * averageSecond)

      CorrelationState(
        numRows,
        averageFirst,
        averageSecond,
        ck,
        xmk,
        ymk)
    }
  }

  override protected def additionalPreconditions(): Seq[Table => Unit] = {
    hasColumn(firstColumn) :: isNumeric(firstColumn) :: hasColumn(secondColumn) ::
      isNumeric(secondColumn):: hasNoInjection(where) :: Nil
  }
}
