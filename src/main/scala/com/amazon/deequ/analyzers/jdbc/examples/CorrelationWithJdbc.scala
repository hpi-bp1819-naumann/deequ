package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcCorrelation, Table}

object CorrelationWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val correlationOfFatFactor = JdbcCorrelation("fat_factor",
      "cho_factor", Some("cho_factor < 2.0")).calculate(table)

    println(correlationOfFatFactor)
  }
}
