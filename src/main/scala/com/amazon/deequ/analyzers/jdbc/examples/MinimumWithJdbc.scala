package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Minimum
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object MinimumWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val minimumOfFatFactor = Minimum("fat_factor", Some("fat_factor > 5.0")).calculate(table)

    println(minimumOfFatFactor)
  }
}
