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

import com.amazon.deequ.examples.ExampleUtils.withSpark
import com.amazon.deequ.sanitychecker.{SanityCheckerRunner, SanityReport}

private[examples] object OneClickSanityCheckExample extends App {

  withSpark { session =>

    /* We profile raw data, mostly in string format (e.g., from a csv file) */
    val rows = session.sparkContext.parallelize(Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "20", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false")
    ))

    val rawData = session.createDataFrame(rows)

    /* Make deequ perform basic checks on this data. */
    val result = SanityCheckerRunner()
      .onData(rawData)
      .withLabel("valuable")
      .withExactDistinctCountForColumns(Seq("status"))
      .withColumnWhitelists(Map("valuable" -> Seq("true", "false")))
      .withColumnBlacklists(Map("status" -> Seq("UNKNOWN"), "name" -> Seq("thingZ")))
      .run()

    SanityReport.print(result)
    println(SanityReport.toJson(result))
  }
}
