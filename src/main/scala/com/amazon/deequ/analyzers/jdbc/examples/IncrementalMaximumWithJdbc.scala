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

package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.MaxState
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcFileSystemStateProvider, JdbcMaximum, Table}

object IncrementalMaximumWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val analyzer = JdbcMaximum("fat_factor")

    val maximum1 = analyzer.calculate(table)

    val maxState = analyzer.computeStateFrom(table).getOrElse(MaxState(0))

    val stateProvider = JdbcFileSystemStateProvider("") // file system path to store at
    stateProvider.persist[MaxState](analyzer, maxState)

    val maximum2 = analyzer.computeMetricFrom(stateProvider.load[MaxState](analyzer))

    println(maximum1)
    println(maximum2)
  }
}
