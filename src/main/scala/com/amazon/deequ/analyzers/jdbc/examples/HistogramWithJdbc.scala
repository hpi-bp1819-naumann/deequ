package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Histogram
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object HistogramWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val customBinner = "WHEN fat_factor < 8.5 OR fat_factor >= 8.5 THEN 'Value1' ELSE 'Value2'"
    val histogramOfComName = Histogram("fat_factor").calculate(table)

    println(histogramOfComName)
  }
}
