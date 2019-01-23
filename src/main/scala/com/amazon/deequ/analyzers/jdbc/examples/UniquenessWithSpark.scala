package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Uniqueness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object UniquenessWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())
    val uniquenessOfFatFactor = Uniqueness("fat_factor").calculate(data)

    println(uniquenessOfFatFactor)
  }
}
