/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ
package examples

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.amazon.deequ.runtime.jdbc.JdbcHelpers
import com.amazon.deequ.runtime.jdbc.operators._
import org.sqlite.Function

import scala.io.Source


object ExampleUtils {

  // This uses a PostreSQL connection for computation
  def withJdbc(func: Connection => Unit): Unit = {

    val jdbcUrl = "jdbc:postgresql://localhost:5432/food"
    val connection = DriverManager.getConnection(jdbcUrl, connectionProperties())

    try {
      func(connection)
    } finally {
      connection.close()
    }
  }

  def connectionProperties(): Properties = {

    val url = getClass.getResource("/jdbc.properties")

    if (url == null) {
      throw new IllegalStateException("Unable to find jdbc.properties in src/main/resources!")
    }

    val properties = new Properties()
    properties.load(Source.fromURL(url).bufferedReader())

    properties
  }

  // This uses SQLite for computation
  def withJdbcForSQLite(testFunc: Connection => Unit): Unit = {

    val jdbcUrlSQLite = "jdbc:sqlite:analyzerTests.db?mode=memory&cache=shared"
    val connection = DriverManager.getConnection(jdbcUrlSQLite)

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

  def itemsAsTable(connection: Connection, items: Item*): Table = {

    val schema = JdbcStructType(
      JdbcStructField("id", LongType) ::
        JdbcStructField("name", StringType) ::
        JdbcStructField("description", StringType) ::
        JdbcStructField("priority", StringType) ::
        JdbcStructField("numViews", LongType) :: Nil)

    val rowData = items.map(item => Seq(item.id, item.name, item.description, item.priority, item.numViews))

    JdbcHelpers.fillTableWithData("exampleItems", schema, rowData, connection, temporary = true)
  }

  def manufacturersAsTable(connection: Connection, manufacturers: Manufacturer*): Table = {

    val schema = JdbcStructType(
      JdbcStructField("id", LongType) ::
        JdbcStructField("name", StringType) ::
        JdbcStructField("countryCode", StringType) :: Nil)

    val rowData = manufacturers.map(manufacturer => Seq(manufacturer.id, manufacturer.name, manufacturer.countryCode))

    JdbcHelpers.fillTableWithData("exampleItems", schema, rowData, connection, temporary = true)
  }
}
