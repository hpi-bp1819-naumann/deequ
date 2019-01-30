package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.StandardDeviation
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object StandardDeviationWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val standardDeviationOfFatFactor = StandardDeviation("fat_factor").calculate(data)

    println(standardDeviationOfFatFactor)
  }
}
