package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Uniqueness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object UniquenessWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val uniquenessOfFatFactor = Uniqueness("fat_factor").calculate(table)

    println(uniquenessOfFatFactor)
  }
}
