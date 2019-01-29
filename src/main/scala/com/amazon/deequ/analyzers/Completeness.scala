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

import Analyzers.ifNoNullsIn
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzers, JdbcPreconditions, Table}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{IntegerType, StructType}

/** Completeness is the fraction of non-null values in a column of a DataFrame. */
case class Completeness(column: String, where: Option[String] = None) extends
  StandardScanShareableAnalyzer[NumMatchesAndCount]("Completeness", column) {

  override def fromAggregationResult(result: AggregationResult, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctionsWithSpark(): Seq[Column] = {

    val summation = sum(Analyzers.conditionalSelection(column, where).isNotNull.cast(IntegerType))

    summation :: Analyzers.conditionalCount(where) :: Nil
  }

  override def aggregationFunctionsWithJdbc(): Seq[String] = {

    val summation = s"COUNT(${JdbcAnalyzers.conditionalSelectionNotNull(column, where)})"

    summation :: JdbcAnalyzers.conditionalCount(where) :: Nil
  }

  override protected def additionalPreconditionsWithSpark(): Seq[StructType => Unit] = {
    Preconditions.hasColumn(column) :: Nil
  }

  override protected def additionalPreconditionsWithJdbc(): Seq[Table => Unit] = {
    JdbcPreconditions.hasColumn(column) :: Nil
  }
}
