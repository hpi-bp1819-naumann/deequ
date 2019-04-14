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

package com.amazon.deequ.utils

import java.sql.{Connection, Timestamp}
import java.util.UUID

import com.amazon.deequ.runtime.jdbc.operators.{JdbcStructField, _}

import scala.util.Random

trait FixtureSupport {

  def hasTable(conn: Connection, tableName: String): Boolean = {
    val query =
      s"""
         |SELECT
         |  name
         |FROM
         |  sqlite_master
         |WHERE
         |  type='table'
         |  AND name='$tableName'
       """.stripMargin

    val stmt = conn.createStatement()
    val result = stmt.executeQuery(query)

    if (result.next()) {
      true
    } else {
      false
    }
  }

  def randomUUID(): String = {
    UUID.randomUUID().toString.replace("-", "")
  }

  def getDefaultTableWithName(tableName: String, connection: Connection): Table = {
    Table(s"${tableName}_${randomUUID()}", connection)
  }

  def fillTableWithData(tableName: String,
                        schema: JdbcStructType, 
                        values: Seq[Seq[Any]],
                        connection: Connection
                       ): Table = {

    val table = getDefaultTableWithName(tableName, connection)

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | ${table.name}
       """.stripMargin
    table.execute(deletionQuery)

    val creationQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS
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
           | ${schema.columnNames()}
           |VALUES
           | $sqlValues
         """.stripMargin

      table.execute(insertQuery)
    }
    table
  }

  def deleteTable(table: Table): Unit = {

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | ${table.name}
       """.stripMargin

    val stmt = table.jdbcConnection.createStatement()
    stmt.execute(deletionQuery)
  }

  def getTableWithNRows(connection: Connection, n: Int): Table = {

    val schema = JdbcStructType(
      JdbcStructField("c0", StringType) ::
        JdbcStructField("c1", StringType) ::
        JdbcStructField("c2", StringType) :: Nil)
    
    val data =
      (1 to n)
        .toList
        .map { index => Seq(s"$index", s"c1-r$index", s"c2-r$index")}
    
    fillTableWithData("NRows", schema, data, connection)
  }

  def getTableMissingColumnWithSize(connection: Connection): (Table, Long) = {

    val schema = JdbcStructType(
      JdbcStructField("item", IntegerType) ::
        JdbcStructField("att1", IntegerType) :: Nil)

    val data =
      Seq(
        Seq(1, null),
        Seq(2, null),
        Seq(3, null))

    (fillTableWithData("MissingColumn", schema, data, connection), data.size)
  }

  def getTableMissingColumn(connection: Connection): Table = {
    getTableMissingColumnWithSize(connection)._1
  }

  def getTableEmptyWithSize(connection: Connection): (Table, Long) = {

    val schema = JdbcStructType(
      JdbcStructField("item", IntegerType) ::
        JdbcStructField("att1", IntegerType) :: Nil)

    val data = Seq()

    (fillTableWithData("EmptyTable", schema, data, connection), 0)
  }

  def getTableEmpty(connection: Connection): Table = {
    getTableEmptyWithSize(connection)._1
  }

  def getTableMissingWithSize(connection: Connection): (Table, Long) = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "a", "f"),
        Seq("2", "b", "d"),
        Seq("3", null, "f"),
        Seq("4", "a", null),
        Seq("5", "a", "f"),
        Seq("6", null, "d"),
        Seq("7", null, "d"),
        Seq("8", "b", null),
        Seq("9", "a", "f"),
        Seq("10", null, null),
        Seq("11", null, "f"),
        Seq("12", null, "d")
      )
    (fillTableWithData("Missing", schema, data, connection), data.size)
  }

  def getTableMissing(connection: Connection): Table = {
    getTableMissingWithSize(connection)._1
  }

  def getTableFullWithSize(connection: Connection): (Table, Long) = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "a", "c"),
        Seq("2", "a", "c"),
        Seq("3", "a", "c"),
        Seq("4", "b", "d")
      )
    (fillTableWithData("Full", schema, data, connection), data.size)
  }

  def getTableFull(connection: Connection): Table = {
    getTableFullWithSize(connection)._1
  }

  def getTableWithNegativeNumbers(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "-1", "-1.0"),
        Seq("2", "-2", "-2.0"),
        Seq("3", "-3", "-3.0"),
        Seq("4", "-4", "-4.0")
      )
    fillTableWithData("NegativeNumbers", schema, data, connection)
  }

  def getTableCompleteAndInCompleteColumns(connection: Connection): Table = {
    getTableCompleteAndInCompleteColumnsWithSize(connection)._1
  }

  def getTableCompleteAndInCompleteColumnsWithSize(connection: Connection): (Table, Int) = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "a", "f"),
        Seq("2", "b", "d"),
        Seq("3", "a", null),
        Seq("4", "a", "f"),
        Seq("5", "b", null),
        Seq("6", "a", "f")
      )

    (fillTableWithData("CompleteAndInCompleteColumns", schema, data, connection), data.size)
  }

  def getTableCompleteAndInCompleteColumnsDelta(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("7", "a", null),
        Seq("8", "b", "d"),
        Seq("9", "a", null)
      )
    fillTableWithData("CompleteAndInCompleteColumns", schema, data, connection)
  }

  def getTableFractionalIntegralTypes(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "1.0"),
        Seq("2", "1")
      )
    fillTableWithData("FractionalIntegralTypes", schema, data, connection)
  }

  def getTableFractionalStringTypes(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "1.0"),
        Seq("2", "a")
      )
    fillTableWithData("FractionalStringTypes", schema, data, connection)
  }

  def getTableIntegralStringTypes(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "1"),
        Seq("2", "a")
      )
    fillTableWithData("IntegralStringTypes", schema, data, connection)
  }

  def getTableWithNumericValues(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", IntegerType) ::
        JdbcStructField("att2", IntegerType) :: Nil)

    val data =
      Seq(
        Seq("1", 1, 0),
        Seq("2", 2, 0),
        Seq("3", 3, 0),
        Seq("4", 4, 5),
        Seq("5", 5, 6),
        Seq("6", 6, 7)
      )
    fillTableWithData("NumericValues", schema, data, connection)
  }

  def getTableWithNumericFractionalValues(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", FloatType) ::
        JdbcStructField("att2", FloatType) :: Nil)

    val data =
      Seq(
        Seq("1", 1.0, 0.0),
        Seq("2", 2.0, 0.0),
        Seq("3", 3.0, 0.0),
        Seq("4", 4.0, 5.0),
        Seq("5", 5.0, 6.0),
        Seq("6", 6.0, 7.0)
      )
    fillTableWithData("NumericFractionalValues", schema, data, connection)
  }

  def getTableWithUniqueColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("uniqueValues", StringType) ::
        JdbcStructField("nonUniqueValues", StringType) ::
        JdbcStructField("nonUniqueWithNulls", StringType) ::
        JdbcStructField("uniqueWithNulls", StringType) ::
        JdbcStructField("onlyUniqueWithOtherNonUnique", StringType) ::
        JdbcStructField("halfUniqueCombinedWithNonUnique", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "0", "3", "1", "5", "0"),
        Seq("2", "0", "3", "2", "6", "0"),
        Seq("3", "0", "3", null, "7", "0"),
        Seq("4", "5", null, "3", "0", "4"),
        Seq("5", "6", null, "4", "0", "5"),
        Seq("6", "7", null, "5", "0", "6")
      )
    fillTableWithData("UniqueColumns", schema, data, connection)
  }

  def getTableWithDistinctValues(connection: Connection): Table = {

    val schema = JdbcStructType(
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("a", null),
        Seq("a", null),
        Seq(null, "x"),
        Seq("b", "x"),
        Seq("b", "x"),
        Seq("c", "y")
      )
    fillTableWithData("DistinctValues", schema, data, connection)
  }

  def getTableWithEmptyStringValues(connection: Connection): Table = {

    val schema = JdbcStructType(
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", ""),
        Seq("2", ""),
        Seq("3", "x"),
        Seq("4", "x"),
        Seq("5", "x"),
        Seq("6", "")
      )
    fillTableWithData("EmptyStringValues", schema, data, connection)
  }

  def getTableWithWhitespace(connection: Connection): Table = {

    val schema = JdbcStructType(
        JdbcStructField("att1", StringType) ::
        JdbcStructField("att2", StringType) :: Nil)

    val data =
      Seq(
        Seq("1", "x"),
        Seq("2", " "),
        Seq("3", " "),
        Seq("4", " "),
        Seq("5", "x"),
        Seq("6", "x")
      )
    fillTableWithData("Whitespace", schema, data, connection)
  }

  def getTableWithConditionallyUninformativeColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("att1", IntegerType) ::
        JdbcStructField("att2", IntegerType) :: Nil)

    val data =
      Seq(
        Seq(1, 0),
        Seq(2, 0),
        Seq(3, 0)
      )
    fillTableWithData("ConditionallyUninformativeColumns", schema, data, connection)
  }

  def getTableWithConditionallyInformativeColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("att1", IntegerType) ::
        JdbcStructField("att2", IntegerType) :: Nil)

    val data =
      Seq(
        Seq(1, 4),
        Seq(2, 5),
        Seq(3, 6)
      )
    fillTableWithData("ConditionallyInformativeColumns", schema, data, connection)
  }

  def getTableWithImproperDataTypes(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("mixed", StringType) ::
        JdbcStructField("type_integer", IntegerType) ::
        JdbcStructField("type_fractional", DoubleType) :: Nil)

    val data =
      Seq(
        Seq("-1", 1, 2.3),
        Seq("+2.376", 2, 5.6),
        Seq("true", 3, null),
        Seq("null", null, null),
        Seq("string", 6, 3.3),
        Seq("null", 6, 3.3)
      )
    fillTableWithData("MissingTypes", schema, data, connection)
  }

  def getTableWithInverseNumberedColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("att1", IntegerType) ::
        JdbcStructField("att2", IntegerType) :: Nil)

    val data =
      Seq(
        Seq(1, 6),
        Seq(2, 5),
        Seq(3, 4)
      )
    fillTableWithData("ConditionallyInformativeColumns", schema, data, connection)
  }

  def getTableWithPartlyCorrelatedColumns(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("att1", IntegerType) ::
        JdbcStructField("att2", IntegerType) :: Nil)

    val data =
      Seq(
        Seq(1, 4),
        Seq(2, 2),
        Seq(3, 6)
      )
    fillTableWithData("ConditionallyInformativeColumns", schema, data, connection)
  }

  def getTableWithPricedItems(connection: Connection): Table = {

    val schema = JdbcStructType(
      JdbcStructField("item", StringType) ::
        JdbcStructField("att1", StringType) ::
        JdbcStructField("count", IntegerType) ::
        JdbcStructField("price", FloatType) :: Nil)

    val data =
      Seq(
        Seq("1", "a", 17, 1.3),
        Seq("2", null, 12, 76.0),
        Seq("3", "b", 15, 89.0),
        Seq("4", "b", 12, 12.7),
        Seq("5", null, 1, 1.0),
        Seq("6", "a", 21, 78.0),
        Seq("7", null, 12, 0.0)
      )
    fillTableWithData("PricedItems", schema, data, connection)
  }

  def getTableWithCategoricalColumn(
    connection: Connection,
    numberOfRows: Int,
    categories: Seq[String])
  : Table = {

    val random = new Random(0)

    val schema = JdbcStructType(
        JdbcStructField("att1", StringType) ::
        JdbcStructField("categoricalColumn", StringType) :: Nil)

    val rowData =
      (1 to numberOfRows)
        .toList
        .map { index => Seq(s"$index", random.shuffle(categories).head)}

    fillTableWithData("categoricalColumn", schema, rowData, connection)
  }
}
