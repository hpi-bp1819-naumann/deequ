package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.analyzers.jdbc.Table
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._

object CompletenessWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val completenessOfFatFactor = Completeness("fat_factor",
      Some("cho_factor < 2.0")).calculate(table)

    println(completenessOfFatFactor)
  }
}
