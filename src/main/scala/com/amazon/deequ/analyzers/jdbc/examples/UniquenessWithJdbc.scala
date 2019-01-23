package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Uniqueness
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl}
import com.amazon.deequ.analyzers.jdbc.Table

object UniquenessWithJdbc extends App {

  val table = Table("food_des", jdbcUrl, connectionProperties())
  val uniquenessOfFatFactor = Uniqueness("fat_factor").calculate(table)

  println(uniquenessOfFatFactor)
}
