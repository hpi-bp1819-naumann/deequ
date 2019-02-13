package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcCountDistinct, Table}

object CountDistinctWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val countDistinctOfFatFactor = JdbcCountDistinct("fat_factor").calculate(table)

    println(countDistinctOfFatFactor)
  }
}
