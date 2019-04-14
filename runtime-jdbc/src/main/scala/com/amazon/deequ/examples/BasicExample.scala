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

package com.amazon.deequ.examples

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.examples.ExampleUtils.withJdbc
import com.amazon.deequ.runtime.jdbc.JdbcDataset
import com.amazon.deequ.runtime.jdbc.operators.Table

private[examples] object BasicExample extends App {

  withJdbc { connection =>

    val data = Table("example_table", connection)

    val verificationResult = VerificationSuite()
      .onData(JdbcDataset(data))
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // we expect 5 records
          .hasSize(_ == 5)
          // 'id' should never be NULL
          .isComplete("id")
          // 'id' should not contain duplicates
          .isUnique("id")
          // 'name' should never be NULL
          .isComplete("name")
          // 'priority' should only contain the values "high" and "low"
          .isContainedIn("priority", Array("high", "low"))
          // 'numViews' should not contain negative values
          .isNonNegative("numViews")
          )
      .addCheck(
        Check(CheckLevel.Warning, "distribution checks")
          // at least half of the 'description's should contain a url
          .containsURL("description", _ >= 0.5))
      .run()

    if (verificationResult.status == Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }
  }
}
