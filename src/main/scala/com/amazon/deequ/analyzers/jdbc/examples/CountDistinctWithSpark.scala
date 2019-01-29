package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.CountDistinct
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object CountDistinctWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val countDistinctOfFatFactor = CountDistinct("fat_factor").calculate(data)

    println(countDistinctOfFatFactor)
  }
}
