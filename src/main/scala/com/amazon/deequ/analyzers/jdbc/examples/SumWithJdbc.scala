package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Sum
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl}
import com.amazon.deequ.analyzers.jdbc.Table

object SumWithJdbc extends App {

  val table = Table("food_des", jdbcUrl, connectionProperties())
  val sumOfFatFactor = Sum("fat_factor", Some("fat_factor > 5.0")).calculate(table)

  println(sumOfFatFactor)
}
