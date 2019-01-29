package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.PatternMatch
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import com.amazon.deequ.analyzers.jdbc.Table

object PatternMatchWithJdbc extends App {

  withJdbc { connection =>

    val table = Table("data_src", connection)
    val patternMatchOfAuthors = PatternMatch("authors",
      raw"(?i)association|administration|laboratory".r).calculate(table)

    println(patternMatchOfAuthors)
  }
}
