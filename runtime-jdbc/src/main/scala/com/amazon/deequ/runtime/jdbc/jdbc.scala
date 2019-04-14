package com.amazon.deequ.runtime

import java.sql.{Connection, Timestamp}
import java.util.UUID

import com.amazon.deequ.runtime.jdbc.operators.{JdbcDataType, JdbcStructField, JdbcStructType, Table}

package object jdbc {

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

    val stmt = connection.createStatement()
    stmt.execute(deletionQuery)

    val creationQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS
         | ${table.name}
         |  ${schema.toString}
       """.stripMargin

    stmt.execute(creationQuery)

    if (values.nonEmpty) {
      val sqlValues = values.map(row => {
        row.map({
          case value: String => "'" + value + "'"
          case ts: Timestamp => "\"" + ts + "\""
          case value => s"$value"
        }).mkString("""(""", """,""", """)""")
      }).mkString(""",""")

      val insertQuery =
        s"""
           |INSERT INTO ${table.name}
           | ${schema.columnNames()}
           |VALUES
           | $sqlValues
         """.stripMargin

      stmt.execute(insertQuery)
    }
    table
  }
}
