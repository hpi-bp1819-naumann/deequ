package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Mean
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object MeanWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val meanOfFatFactor = Mean("fat_factor", Some("fat_factor < 5.0")).calculate(table)

    println(meanOfFatFactor)
  }
}
