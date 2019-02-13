package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{withSpark, jdbcUrl, connectionProperties}

object ComplianceWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val complianceWithFatFactorRange =
      Compliance(s"constraintName", s"fat_factor BETWEEN 0 AND 1").calculate(data)

    println(complianceWithFatFactorRange)
  }
}
