package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcMaximum, Table}

object MaximumWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val maximumOfFatFactor = JdbcMaximum("fat_factor", Some("fat_factor < 5.0")).calculate(table)

    println(maximumOfFatFactor)
  }
}
