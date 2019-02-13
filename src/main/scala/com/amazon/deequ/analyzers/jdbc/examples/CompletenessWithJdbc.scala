package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcCompleteness, Table}

object CompletenessWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val completenessOfFatFactor = JdbcCompleteness("fat_factor",
      Some("cho_factor < 2.0")).calculate(table)

    println(completenessOfFatFactor)
  }
}
