package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Histogram
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object HistogramWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val histogramOfComName = Histogram("comname").calculate(data)

    println(histogramOfComName)
  }
}
