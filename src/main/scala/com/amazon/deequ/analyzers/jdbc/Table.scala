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

import java.io.FileReader
import java.sql.Connection

import scala.io.Source
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

case class Table(
  name: String,
  jdbcConnection: Connection) {

  def this(name: String, jdbcConnection: Connection, csvFile: String) = {
    this(name, jdbcConnection)
    createTableFromCSV(csvFile)
    insertCSVToTable(csvFile)
  }

  def insertCSVToTable(csvFile: String) : Unit = {
    val copMan = new CopyManager(this.jdbcConnection.asInstanceOf[BaseConnection])
    val fileReader = new FileReader(csvFile)
    copMan.copyIn("COPY " + this.name + " FROM STDIN DELIMITER ';' CSV HEADER", fileReader)
  }

  def createTableFromCSV(csvFile: String) : Unit = {
    val src = Source.fromFile(csvFile)
    val columnString = src.getLines().next().split(";").map(head => head + " text").mkString(",")

    val queryString = s"""
                         |CREATE TABLE IF NOT EXISTS
                         | ${this.name}
                         | ($columnString)
      """.stripMargin

    val statement = this.jdbcConnection.prepareStatement(queryString)
    statement.execute()
  }

}
