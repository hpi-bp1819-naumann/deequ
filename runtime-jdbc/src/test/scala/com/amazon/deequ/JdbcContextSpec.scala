package com.amazon.deequ

import java.sql.{Connection, DriverManager}

import org.sqlite.Function

/**
  * To be mixed with Tests so they can use a default spark context suitable for testing
  */
trait JdbcContextSpec {

  val jdbcUrl = "jdbc:sqlite:analyzerTests.db?mode=memory&cache=shared"

  def withJdbc(testFunc: Connection => Unit): Unit = {

    val connection = DriverManager.getConnection(jdbcUrl)

    // Register user defined function for regular expression matching
    Function.create(connection, "regexp_matches", new Function() {
      protected def xFunc(): Unit = {
        val textToMatch = value_text(0)
        val pattern = value_text(1).r

        pattern.findFirstMatchIn(textToMatch) match {
          case Some(_) => result(1) // If a match is found, return any value other than NULL
          case None => result() // If no match is found, return NULL
        }
      }
    })

    // Register user defined function for natural logarithm
    Function.create(connection, "ln", new Function() {
      protected def xFunc(): Unit = {
        val num = value_double(0)

        if (num != 0) {
          result(math.log(num))
        } else {
          result()
        }
      }
    })

    try {
      testFunc(connection)
    } finally {
      connection.close()
    }
  }
}
