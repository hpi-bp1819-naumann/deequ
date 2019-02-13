package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcHistogram, Table}

object HistogramWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val histogramOfComName = JdbcHistogram("fat_factor").calculate(table)

    println(histogramOfComName)
  }
}
