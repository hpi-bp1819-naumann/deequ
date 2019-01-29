package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Maximum
import com.amazon.deequ.analyzers.jdbc.Table
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._

object MaximumWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val maximumOfFatFactor = Maximum("fat_factor", Some("fat_factor < 5.0")).calculate(table)

    println(maximumOfFatFactor)
  }
}
