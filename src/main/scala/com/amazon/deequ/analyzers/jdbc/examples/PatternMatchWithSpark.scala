package com.amazon.deequ.analyzers.jdbc.examples

import com.amazon.deequ.analyzers.PatternMatch
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object PatternMatchWithSpark extends App {

  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "data_src", connectionProperties())

    val patternMatchOfAuthors = PatternMatch("authors",
      raw"(?i)association|administration|laboratory".r).calculate(data)

    println(patternMatchOfAuthors)
  }
}
