package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcStandardDeviation, Table}

object StandardDeviationWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val standardDeviationOfFatFactor = JdbcStandardDeviation("fat_factor",
      Some("fat_factor < 5.0")).calculate(table)

    println(standardDeviationOfFatFactor)
  }
}
