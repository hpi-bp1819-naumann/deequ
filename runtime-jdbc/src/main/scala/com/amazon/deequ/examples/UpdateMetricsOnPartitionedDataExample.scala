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

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.examples.ExampleUtils.{manufacturersAsTable, withJdbcForSQLite}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.executor.JdbcExecutor
import com.amazon.deequ.runtime.jdbc.operators.{Operator, State}
import com.amazon.deequ.runtime.jdbc.{InMemoryJdbcStateProvider, JdbcEngine}

object UpdateMetricsOnPartitionedDataExample extends App {

  /* NOTE: Stateful support is still work in progress, and is therefore not yet integrated into
   VerificationSuite. We showcase however how to incrementally compute metrics on a growing
   dataset using the AnalysisRunner. */

  /*
  * to use a PostgreSQL connection for computation please
  * change withJdbcForSQLite in the next line to withJdbc
  */
  withJdbcForSQLite { connection =>

    // Assume we store and process our data in a partitioned manner:
    // In this example, we operate on a table of manufacturers partitioned by country code
    val deManufacturers = manufacturersAsTable(connection,
      Manufacturer(1, "ManufacturerA", "DE"),
      Manufacturer(2, "ManufacturerB", "DE"))

    val usManufacturers = manufacturersAsTable(connection,
      Manufacturer(3, "ManufacturerD", "US"),
      Manufacturer(4, "ManufacturerE", "US"),
      Manufacturer(5, "ManufacturerF", "US"))

    val cnManufacturers = manufacturersAsTable(connection,
      Manufacturer(6, "ManufacturerG", "CN"),
      Manufacturer(7, "ManufacturerH", "CN"))

    // We are interested in the the following constraints of the table as a whole
    val check = Check(CheckLevel.Warning, "a check")
        .isComplete("name")
        .containsURL("name", _ == 0.0)
        .isContainedIn("countryCode", Array("DE", "US", "CN"))

    // Deequ now allows us to compute states for the metrics on which the constraints are defined
    // according to the partitions of the data.

    val analyzers = check.requiredAnalyzers().toSeq
    val analysis = analyzers.map { JdbcEngine.matchingOperator }.asInstanceOf[Seq[Operator[State[_], Metric[_]]]]

    // We first compute and store the state per partition
    val deStates = InMemoryJdbcStateProvider()
    val usStates = InMemoryJdbcStateProvider()
    val cnStates = InMemoryJdbcStateProvider()

    JdbcExecutor.run(deManufacturers, analysis, saveStatesWith = Some(deStates))
    JdbcExecutor.run(usManufacturers, analysis, saveStatesWith = Some(usStates))
    JdbcExecutor.run(cnManufacturers, analysis, saveStatesWith = Some(cnStates))

    // Next, we compute the metrics for the whole table from the partition states
    // Note that we do not need to touch the data again, the states are sufficient
    val tableMetrics = JdbcExecutor.runOnAggregatedStates(deManufacturers, analysis,
      Seq(deStates, usStates, cnStates))

    println("Metrics for the whole table:\n")
    tableMetrics.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    // Lets now assume that a single partition changes. We only need to recompute the state of this
    // partition in order to update the metrics for the whole table.

    val updatedUsManufacturers = manufacturersAsTable(connection,
      Manufacturer(3, "ManufacturerDNew", "US"),
      Manufacturer(4, null, "US"),
      Manufacturer(5, "ManufacturerFNew http://clickme.com", "US"))

    // Recompute state of partition
    val updatedUsStates = InMemoryJdbcStateProvider()

    JdbcExecutor.run(updatedUsManufacturers, analysis, saveStatesWith = Some(updatedUsStates))

    // Recompute metrics for whole tables from states. We do not need to touch old data!
    val updatedTableMetrics = JdbcExecutor.runOnAggregatedStates(deManufacturers, analysis,
      Seq(deStates, updatedUsStates, cnStates))

    println("Metrics for the whole table after updating the US partition:\n")
    updatedTableMetrics.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }
  }
}
