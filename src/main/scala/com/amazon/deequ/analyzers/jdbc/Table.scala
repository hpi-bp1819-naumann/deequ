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

import java.sql.{Connection, ResultSet}

case class Table(
  name: String,
  jdbcConnection: Connection) {

  /**
    * Builds and executes SQL statement and returns the ResultSet
    *
    * @param aggregations Sequence of aggregation functions
    * @return Returns ResultSet of the query
    */
  def executeAggregations(aggregations: Seq[String]): ResultSet = {
    val query =
      s"""
         |SELECT
         | ${aggregations.mkString(", ")}
         |FROM
         | $name
      """.stripMargin

    val result = jdbcConnection.createStatement().executeQuery(query)
    // TODO: Test return value of next() and throw exception
    result.next()
    result
  }
}
