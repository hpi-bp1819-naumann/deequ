package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object SizeWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val sizeOfTable = Size(Some("fat_factor < 5.0")).calculate(table)

    println(sizeOfTable)
  }
}
