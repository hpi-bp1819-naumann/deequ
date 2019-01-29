package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.UniqueValueRatio
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object UniqueValueRatioWithSpark extends App {
  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val uniqueValueRatioOfFatFactor = UniqueValueRatio("fat_factor").calculate(data)

    println(uniqueValueRatioOfFatFactor)
  }
}
