package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Distinctness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object DistinctnessWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val distinctnessOfFatFactor = Distinctness("fat_factor").calculate(data)

    println(distinctnessOfFatFactor)
  }
}
