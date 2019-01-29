package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.DataType
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object DataTypeWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val dataTypeOfFatFactor = DataType("fat_factor", Some("fat_factor > 8.8")).calculate(table)

    println(dataTypeOfFatFactor)
  }
}
