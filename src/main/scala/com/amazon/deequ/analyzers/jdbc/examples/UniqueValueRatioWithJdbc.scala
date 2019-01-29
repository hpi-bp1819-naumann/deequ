package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.UniqueValueRatio
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object UniqueValueRatioWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val uniqueValueRatioOfFatFactor = UniqueValueRatio("fat_factor").calculate(table)

    println(uniqueValueRatioOfFatFactor)
  }
}
