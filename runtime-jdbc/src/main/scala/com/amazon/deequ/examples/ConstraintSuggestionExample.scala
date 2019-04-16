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
package examples

import com.amazon.deequ.examples.ExampleUtils._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{JdbcDataset, JdbcHelpers}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

private[examples] object ConstraintSuggestionExample extends App {

  /*
  * to use a PostgreSQL connection for computation please
  * change withJdbcForSQLite in the next line to withJdbc
  */
  withJdbcForSQLite { connection =>

    val schema = JdbcStructType(
      JdbcStructField("name", StringType) ::
        JdbcStructField("count", StringType) ::
        JdbcStructField("status", StringType) ::
        JdbcStructField("valuable", StringType) :: Nil)

    // Lets first generate some example data
    val rows = Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "24", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false"),
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "17.0", "UNKNOWN", null),
      RawData("thingC", "22", "UNKNOWN", null),
      RawData("thingE", "23", "DELAYED", "false")
    ).map(row => Seq(row.name, row.count, row.status, row.valuable))

    val data = JdbcDataset(
      JdbcHelpers.fillTableWithData(
      "ConstraintSuggestionExampleData", schema,
        rows, connection, temporary = true))

    // We ask deequ to compute constraint suggestions for us on the data
    // It will profile the data and than apply a set of rules specified in addConstraintRules()
    // to suggest constraints
    val suggestionResult = ConstraintSuggestionRunner()
      .onData(data)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    // We can now investigate the constraints that deequ suggested. We get a textual description
    // and the corresponding scala code for each suggested constraint
    //
    // Note that the constraint suggestion is based on heuristic rules and assumes that the data it
    // is shown is 'static' and correct, which might often not be the case in the real world.
    // Therefore the suggestions should always be manually reviewed before being applied in real
    // deployments.
    suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
      suggestions.foreach { suggestion =>
        println(s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
          s"The corresponding scala code is ${suggestion.codeForConstraint}\n")
      }
    }

  }
}
