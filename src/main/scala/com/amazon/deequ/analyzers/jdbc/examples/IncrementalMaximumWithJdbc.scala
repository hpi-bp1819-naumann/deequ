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
