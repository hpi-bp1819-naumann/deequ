package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.{JdbcDataType, Table}

object DataTypeWithJdbc extends App {

  withJdbc { connection =>
    val table = Table("food_des", connection)
    val dataTypeOfFatFactor = JdbcDataType("fat_factor", Some("fat_factor > 8.8")).calculate(table)

    println(dataTypeOfFatFactor)
  }
}
