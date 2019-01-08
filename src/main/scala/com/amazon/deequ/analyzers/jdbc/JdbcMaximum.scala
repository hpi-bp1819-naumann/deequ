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

import java.sql.ResultSet
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.MaxState
import JdbcAnalyzers._


case class JdbcMaximum(column: String, where: Option[String] = None)
  extends JdbcStandardScanShareableAnalyzer[MaxState]("Maximum", column) {

  override def aggregationFunctions(): Seq[String] = {
    s"MAX(${conditionalSelection(column, where)})" :: Nil
  }

  override def fromAggregationResult(result: ResultSet, offset: Int): Option[MaxState] = {

    ifNoNullsIn(result, offset) { _ =>
      MaxState(result.getDouble(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[Table => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }
}
