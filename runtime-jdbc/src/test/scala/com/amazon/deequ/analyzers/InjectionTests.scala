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

package com.amazon.deequ.analyzers

import java.sql.Connection

import com.amazon.deequ.JdbcContextSpec
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{JdbcEngine, JdbcHelpers}
import com.amazon.deequ.statistics._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class InjectionTests extends WordSpec with Matchers with JdbcContextSpec
  with FixtureSupport {

  "prevent sql injections" should {

    "prevent sql injection in table name" in withJdbc { connection =>

      preventInjectionsInTableName(connection, Completeness("att1"))
      preventInjectionsInTableName(connection, Compliance("att1", "att1 = 1"))
      preventInjectionsInTableName(connection, Correlation("att1", "att2"))
      preventInjectionsInTableName(connection, CountDistinct(Seq("att1")))
      preventInjectionsInTableName(connection, DataType("att1"))
      preventInjectionsInTableName(connection, Distinctness(Seq("att1")))
      preventInjectionsInTableName(connection, Entropy("att1"))
      preventInjectionsInTableName(connection, Histogram("att1"))
      preventInjectionsInTableName(connection, Maximum("att1"))
      preventInjectionsInTableName(connection, Mean("att1"))
      preventInjectionsInTableName(connection, Minimum("att1"))
      preventInjectionsInTableName(connection, PatternMatch("att1", """\d""".r))
      preventInjectionsInTableName(connection, Size())
      preventInjectionsInTableName(connection, StandardDeviation("att1"))
      preventInjectionsInTableName(connection, Sum("att1"))
      preventInjectionsInTableName(connection, Uniqueness(Seq("att1")))
      preventInjectionsInTableName(connection, UniqueValueRatio(Seq("att1")))
    }

    "prevent sql injection in column name" in withJdbc { connection =>

      val table = getTableWithNumericValues(connection)
      val columnWithInjection = s"1 THEN 1 ELSE 0); DROP TABLE ${table.name};"
      assertPreventInjectionsInColumnName(table, Completeness(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Compliance(columnWithInjection, s"$columnWithInjection = 1"))
      assertPreventInjectionsInColumnName(table, Correlation(columnWithInjection, columnWithInjection))
      assertPreventInjectionsInColumnName(table, CountDistinct(Seq(columnWithInjection)))
      assertPreventInjectionsInColumnName(table, DataType(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Distinctness(Seq(columnWithInjection)))
      assertPreventInjectionsInColumnName(table, Entropy(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Histogram(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Maximum(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Mean(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Minimum(columnWithInjection))
      assertPreventInjectionsInColumnName(table, PatternMatch(columnWithInjection, """\d""".r))
      assertPreventInjectionsInColumnName(table, StandardDeviation(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Sum(columnWithInjection))
      assertPreventInjectionsInColumnName(table, Uniqueness(Seq(columnWithInjection)))
      assertPreventInjectionsInColumnName(table, UniqueValueRatio(Seq(columnWithInjection)))
    }

    "prevent sql injection in where clause" in withJdbc { connection =>

      val table = getTableWithNumericValues(connection)
      val whereClauseWithInjection = Some(s"';DROP TABLE ${table.name};--")
      assertPreventInjectionsInColumnName(table, Completeness("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Compliance("att1", s"att1 = 1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Correlation("att1", "att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, DataType("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Maximum("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Mean("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Minimum("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, PatternMatch("att1", """\d""".r, whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, StandardDeviation("att1", whereClauseWithInjection))
      assertPreventInjectionsInColumnName(table, Sum("att1", whereClauseWithInjection))
    }
  }

  def preventInjectionsInTableName(connection: Connection, statistic: Statistic): Unit = {

    val table = getTableWithNumericValues(connection)
    val tableWithInjection = JdbcHelpers.getDefaultTableWithName(
      s"${table.name}; DROP TABLE ${table.name};", connection)
    val operator = JdbcEngine.matchingOperator(statistic)

    assert(operator.calculate(tableWithInjection).value.isFailure)
    assert(table.exists())
  }


  def assertPreventInjectionsInColumnName(table: Table, statistic: Statistic): Unit = {

    val operator = JdbcEngine.matchingOperator(statistic)

    assert(operator.calculate(table).value.isFailure)
    assert(table.exists())
  }

}
