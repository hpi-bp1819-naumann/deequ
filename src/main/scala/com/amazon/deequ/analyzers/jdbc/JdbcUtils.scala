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

package com.amazon.deequ.analyzers.jdbc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.io.Source

private[jdbc] object JdbcUtils {

  classOf[org.postgresql.Driver]

  val jdbcUrl = "jdbc:postgresql://localhost:5432/testdb_2"

  def connectionProperties(): Properties = {

    val url = getClass.getResource("/jdbc.properties")

    if (url == null) {
      throw new IllegalStateException("Unable to find jdbc.properties in src/main/resources!")
    }

    val properties = new Properties()
    properties.load(Source.fromURL(url).bufferedReader())

    properties
  }

  def withJdbc(func: Connection => Unit): Unit = {
    classOf[org.postgresql.Driver]
    val connection = DriverManager.getConnection(jdbcUrl, connectionProperties())
    try {
      func(connection)
    } finally {
      connection.close()
    }
  }

  def withSpark(func: SparkSession => Unit): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

}
