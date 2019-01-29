package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Distinctness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object DistinctnessWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val distinctnessOfFatFactor = Distinctness("fat_factor").calculate(table)

    println(distinctnessOfFatFactor)
  }
}
