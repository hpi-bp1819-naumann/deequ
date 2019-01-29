package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object SizeWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val sizeOfTable = Size(Some("fat_factor < 5.0")).calculate(data)

    println(sizeOfTable)
  }
}
