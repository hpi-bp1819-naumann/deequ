package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object CompletenessWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val completenessOfFatFactor = Completeness("fat_factor",
      Some("cho_factor < 2.0")).calculate(data)

    println(completenessOfFatFactor)
  }
}
