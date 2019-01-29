package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.Entropy
import com.amazon.deequ.analyzers.jdbc.JdbcUtils._
import com.amazon.deequ.analyzers.jdbc.Table

object EntropyWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("food_des", connection)
    val entropyOfFatFactor = Entropy("fat_factor").calculate(table)

    println(entropyOfFatFactor)
  }
}
