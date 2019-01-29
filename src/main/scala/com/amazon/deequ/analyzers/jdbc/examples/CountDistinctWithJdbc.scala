package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.CountDistinct
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object CountDistinctWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val countDistinctOfFatFactor = CountDistinct("fat_factor").calculate(table)

    println(countDistinctOfFatFactor)
  }
}
