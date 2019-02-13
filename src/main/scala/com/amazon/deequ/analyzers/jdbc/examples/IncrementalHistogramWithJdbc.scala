package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcFileSystemStateProvider, JdbcFrequenciesAndNumRows, JdbcHistogram, Table}

object IncrementalHistogramWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val analyzer = JdbcHistogram("fat_factor")

    println(analyzer.calculate(table))

    val stateProvider = JdbcFileSystemStateProvider("") // file system path to store at

    stateProvider.persist[JdbcFrequenciesAndNumRows](analyzer, analyzer.computeStateFrom(table).get)
    val histogramOfComName =
      analyzer.computeMetricFrom(stateProvider.load[JdbcFrequenciesAndNumRows](analyzer))

    println(histogramOfComName)
  }
}
