package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Minimum
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object MinimumWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val minimumOfFatFactor = Minimum("fat_factor", Some("fat_factor > 5.0")).calculate(data)

    println(minimumOfFatFactor)
  }
}
