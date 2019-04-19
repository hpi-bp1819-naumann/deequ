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

package com.amazon.deequ.runtime.jdbc

import java.sql.{Connection, Timestamp}
import java.util.UUID

import com.amazon.deequ.runtime.jdbc.operators._

private[deequ] object JdbcHelpers {

  def tableWithColumn(
      name: String,
      columnType: JdbcDataType,
      connection: Connection,
      values: Seq[Any]*)
    : Table = {

    val schema = JdbcStructType(
      JdbcStructField(name, columnType) :: Nil)

    fillTableWithData("singleColumn", schema, values, connection)
  }

  def randomUUID(): String = {
    UUID.randomUUID().toString.replace("-", "")
  }

  def getDefaultTableWithName(tableName: String, connection: Connection, temporary: Boolean = false, withRandomSuffix: Boolean = false): Table = {

    val prefix = "__deequ__"
    val suffix = withRandomSuffix match {
      case true => s"_${randomUUID()}"
      case false => ""
    }
    new Table(s"$prefix$tableName$suffix", connection, temporary)
  }

  def fillTableWithData(tableName: String,
                        schema: JdbcStructType,
                        values: Seq[Seq[Any]],
                        connection: Connection,
                        temporary: Boolean = false
                       ): Table = {

    val table = getDefaultTableWithName(tableName, connection, temporary = temporary)

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | ${table.name}
       """.stripMargin

    table.execute(deletionQuery)

    val temp_table = table.temporary match  {
      case true => " TEMPORARY"
      case false => ""
    }

    val creationQuery =
      s"""
         |CREATE$temp_table TABLE IF NOT EXISTS
         | ${table.name}
         |  ${schema.toString}
       """.stripMargin

    table.execute(creationQuery)

    if (values.nonEmpty) {
      val sqlValues = values.map(row => {
        row.map({
          case str: String => "'" + str.replace("'", "''") + "'"
          case bool: Boolean => "'" + bool + "'"
          case ts: Timestamp => "\"" + ts + "\""
          case value => value match {
            case null => s"${null}"
            case _ => value.toString
          }
        }).mkString("(", ", " , ")")
      }).mkString(",")

      val insertQuery =
        s"""
           |INSERT INTO ${table.name}
           | ${schema.columnNamesEncoded()}
           |VALUES
           | $sqlValues
         """.stripMargin

      table.execute(insertQuery)
    }
    table
  }
}
