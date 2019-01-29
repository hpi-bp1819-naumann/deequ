package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object ComplianceWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val complianceWithFatFactorRange =
      Compliance(s"constraint name", s"fat_factor BETWEEN 0 AND 1").calculate(table)

    println(complianceWithFatFactorRange)
  }
}
