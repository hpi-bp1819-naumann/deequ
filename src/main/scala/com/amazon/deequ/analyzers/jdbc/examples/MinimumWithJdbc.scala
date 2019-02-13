package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcMinimum, Table}

object MinimumWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val minimumOfFatFactor = JdbcMinimum("fat_factor", Some("fat_factor > 5.0")).calculate(table)

    println(minimumOfFatFactor)
  }
}
