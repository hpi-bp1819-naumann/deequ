package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.DataType
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object DataTypeWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val dataTypeOfFatFactor = DataType("fat_factor", Some("fat_factor > 8.8")).calculate(data)

    println(dataTypeOfFatFactor)
  }
}
