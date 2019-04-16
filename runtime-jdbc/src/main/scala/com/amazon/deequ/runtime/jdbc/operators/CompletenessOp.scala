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

import com.amazon.deequ.runtime.jdbc.operators.Operators._
import com.amazon.deequ.runtime.jdbc.operators.Preconditions._

/** Completeness is the fraction of non-null values in a column of a DataFrame. */
case class CompletenessOp(column: String, where: Option[String] = None) extends
  StandardScanShareableOperator[NumMatchesAndCount]("Completeness", column) {

  override def fromAggregationResult(result: JdbcRow, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[String] = {

    val summation = s"COUNT(${conditionalSelectionNotNull(column, where)})"

    summation :: conditionalCount(where) :: Nil
  }

  override protected def additionalPreconditions(): Seq[Table => Unit] = {
    hasColumn(column) :: hasNoInjection(where) :: Nil
  }
}
