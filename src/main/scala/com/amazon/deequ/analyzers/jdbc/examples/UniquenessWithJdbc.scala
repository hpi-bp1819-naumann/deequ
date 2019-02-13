package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcUniqueness, Table}

object UniquenessWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val uniquenessOfFatFactor = JdbcUniqueness("fat_factor").calculate(table)

    println(uniquenessOfFatFactor)
  }
}
