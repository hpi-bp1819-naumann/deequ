package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Correlation
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object CorrelationWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val correlationOfFatFactor = Correlation("fat_factor",
      "cho_factor", Some("cho_factor < 2.0")).calculate(data)

    println(correlationOfFatFactor)
  }
}
