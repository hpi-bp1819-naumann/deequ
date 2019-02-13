package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcDistinctness, Table}

object DistinctnessWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val distinctnessOfFatFactor = JdbcDistinctness("fat_factor").calculate(table)

    println(distinctnessOfFatFactor)
  }
}
