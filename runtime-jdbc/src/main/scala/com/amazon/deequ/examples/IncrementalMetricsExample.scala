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

import com.amazon.deequ.Analysis
import com.amazon.deequ.examples.ExampleUtils._
import com.amazon.deequ.runtime.jdbc.{InMemoryJdbcStateProvider, JdbcDataset}
import com.amazon.deequ.statistics.{Completeness, Size}

private[examples] object IncrementalMetricsExample extends App {

  /* NOTE: Stateful support is still work in progress, and is therefore not yet integrated into
     VerificationSuite. We showcase however how to incrementally compute metrics on a growing
     dataset using the AnalysisRunner. */

  /*
  * to use a PostgreSQL connection for computation please
  * change withJdbcForSQLite in the next line to withJdbc
  */
  withJdbcForSQLite { connection =>

    val data = itemsAsTable(connection,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available tomorrow", "low", 0),
      Item(3, "Thing C", null, null, 5))

    val moreData = itemsAsTable(connection,
      Item(4, "Thingy D", null, "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val statistics = Seq(Size()/* TODO approx, ApproxCountDistinct("id")*/, Completeness("name"),
      Completeness("description"))

    val stateStore = InMemoryJdbcStateProvider()

    val metricsForData = Analysis
      .onData(JdbcDataset(data))
      .addStatistics(statistics)
      .saveStatesWith(stateStore)
      .run()

    // We update the metrics now from the stored states without having to access the previous data!
    val metricsAfterAddingMoreData = Analysis
      .onData(JdbcDataset(moreData))
      .addStatistics(statistics)
      .aggregateWith(stateStore)
      .run()

    println("Metrics for the first 3 records:\n")
    metricsForData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    println("\nMetrics after adding 2 more records:\n")
    metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

  }
}
