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

package com.amazon.deequ.sanitychecker

import org.apache.spark.sql.DataFrame

class SanityCheckerRunner {

  def onData(data: DataFrame): SanityCheckerRunBuilder = {
    new SanityCheckerRunBuilder(data)
  }

  private[sanitychecker] def run(
      data: DataFrame,
      options: SanityCheckerOptions,
      restrictToColumns: Option[Seq[String]],
      printStatusUpdates: Boolean)
    : SanityReport = {

    val sanityReport = SanityChecker
      .check(
        data,
        options,
        restrictToColumns,
        printStatusUpdates
      )

    sanityReport
  }

}

object SanityCheckerRunner {

  def apply(): SanityCheckerRunner = {
    new SanityCheckerRunner()
  }
}
