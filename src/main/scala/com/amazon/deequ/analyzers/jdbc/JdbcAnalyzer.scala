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

import java.sql.Types._
import java.sql.{JDBCType, PreparedStatement, ResultSet}

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}

import scala.util.{Failure, Success}

trait JdbcAnalyzer[S <: State[_], +M <: Metric[_]] {

  /**
    * Return the query string of this analyzer
    * @param table database table
    * @return query string to send to the database
    */
  def query(table: Table): String

  /**
    * Insert parameters into prepared statement. Can be overwritten if required.
    * @param statement prepared statement to fill
    * @return filled statement
    */
  def insertParams(statement: PreparedStatement): PreparedStatement = {
    statement
  }

  /**
    * Establishes database connection and executes a query to retrieve the data required
    * for state computation.
    * @param table database table
    * @param customQuery query string to send to the database
    * @return
    */
  def queryData(table: Table, customQuery: Option[String] = None): ResultSet = {
    val connection = table.jdbcConnection
    val statement = connection.prepareStatement(customQuery.getOrElse(query(table)),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    insertParams(statement).executeQuery()
  }

  /**
    * Compute the state from the data (sufficient statistics) from the data
    */
  def computeState(result: ResultSet): Option[S]

  /**
    * Query data and return computed state if computation is possible
    * @param table database table
    * @return
    */
  def computeStateFrom(table: Table): Option[S] = {
    val result = queryData(table)
    val state = computeState(result)
    result.close()
    state
  }

  /**
    * Compute the metric from the state (sufficient statistics)
    * @param state wrapper holding a state of type S (required due to typing issues...)
    * @return
    */
  def computeMetricFrom(state: Option[S]): M

  /**
    * A set of assertions that must hold on the database table
    * @return
    */
  def preconditions: Seq[Table => Unit] = {
    Seq.empty
  }

  def validatePreconditions(table: Table): Unit = {
    val exceptionOption = Preconditions.findFirstFailing(table, this.preconditions)
    if (exceptionOption.nonEmpty) {
      throw exceptionOption.get
    }
  }

  def calculate(table: Table): M = {

    try {
      validatePreconditions(table)
      val state = computeStateFrom(table)
      computeMetricFrom(state)
    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M
}

/** Base class for analyzers that require to group the data by specific columns */
abstract class GroupingAnalyzer[S <: State[_], +M <: Metric[_]] extends JdbcAnalyzer[S, M] {

  /** The columns to group the data by */
  def groupingColumns(): Seq[String]

  /** Ensure that the grouping columns exist in the data */
  override def preconditions: Seq[Table => Unit] = {
    groupingColumns().map { name => Preconditions.hasColumn(name) } ++ super.preconditions
  }
}

/** Helper method to check conditions on the schema of the data */
object Preconditions {

  private[this] val numericDataTypes =
    Set(BIGINT, DECIMAL, DOUBLE, FLOAT, INTEGER, NUMERIC, SMALLINT, TINYINT)

  /* Return the first (potential) exception thrown by a precondition */
  def findFirstFailing(
                        table: Table,
                        conditions: Seq[Table => Unit])
  : Option[Exception] = {

    conditions.map { condition =>
      try {
        condition(table)
        None
      } catch {
        /* We only catch exceptions, not errors! */
        case e: Exception => Some(e)
      }
    }
      .find { _.isDefined }
      .flatten
  }

  /** Specified table exists in the data */
  def hasTable(): Table => Unit = { table =>

    val connection = table.jdbcConnection

    val metaData = connection.getMetaData
    val result = metaData.getTables(null, null, null, Array[String]("TABLE"))

    var hasTable = false

    while (result.next()) {
      if (result.getString("TABLE_NAME") == table.name) {
        hasTable = true
      }
    }

    if (!hasTable) {
      throw new NoSuchTableException(s"Input data does not include table ${table.name}!")
    }
  }

  /** At least one column is specified */
  def atLeastOne(columns: Seq[String]): Table => Unit = { _ =>
    if (columns.isEmpty) {
      throw new NoColumnsSpecifiedException("At least one column needs to be specified!")
    }
  }

  /** Exactly n columns are specified */
  def exactlyNColumns(columns: Seq[String], n: Int): Table => Unit = { _ =>
    if (columns.size != n) {
      throw new NumberOfSpecifiedColumnsException(s"$n columns have to be specified! " +
        s"Currently, columns contains only ${columns.size} column(s): ${columns.mkString(",")}!")
    }
  }

  /** Specified column exists in the table */
  def hasColumn(column: String): Table => Unit = { table =>

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | ${table.name}
         |LIMIT 0
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()
    val metaData = result.getMetaData

    var hasColumn = false

    for (i <- 1 to metaData.getColumnCount) {
      if (metaData.getColumnName(i) == column) {
        hasColumn = true
      }
    }

    if (!hasColumn) {
      throw new NoSuchColumnException(s"Input data does not include column $column!")
    }
  }

  /** data type of specified column */
  def getColumnDataType(table: Table, column: String): Int = {
    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | $column
         |FROM
         | ${table.name}
         |LIMIT 0
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    val metaData = result.getMetaData
    metaData.getColumnType(1)
  }

  /** Specified column has a numeric type */
  def isNumeric(column: String): Table => Unit = { table =>
    val columnDataType = getColumnDataType(table, column)

    val hasNumericType = numericDataTypes.contains(columnDataType)

    if (!hasNumericType) {
      throw new WrongColumnTypeException(s"Expected type of column $column to be one of " +
        s"(${numericDataTypes.map(t => JDBCType.valueOf(t).getName).mkString(",")}), " +
        s"but found ${JDBCType.valueOf(columnDataType).getName} instead!")
    }
  }

  /** Statement has no ; outside of quotation marks (SQL injections) */
  def hasNoInjection(statement: Option[String]): Table => Unit = { _ =>
    val pattern = """("[^"]*"|'[^']*'|[^;'"]*)*"""
    val st = statement.getOrElse("")
    if (!st.matches(pattern)) {
      throw new SQLInjectionException(
        s"In the statement semicolons outside of quotation marks are not allowed")
    }
  }
}

private[deequ] object JdbcAnalyzers {

  /** Merges a sequence of potentially empty states. */
  def merge[S <: State[_]](
                            state: Option[S],
                            anotherState: Option[S],
                            moreStates: Option[S]*)
  : Option[S] = {

    val statesToMerge = Seq(state, anotherState) ++ moreStates

    statesToMerge.reduce { (stateA: Option[S], stateB: Option[S]) =>

      (stateA, stateB) match {
        case (Some(theStateA), Some(theStateB)) =>
          Some(theStateA.sumUntyped(theStateB).asInstanceOf[S])

        case (Some(_), None) => stateA
        case (None, Some(_)) => stateB
        case _ => None
      }
    }
  }

  def entityFrom(columns: Seq[String]): Entity.Value = {
    if (columns.size == 1) Entity.Column else Entity.Mutlicolumn
  }

  def metricFromValue(
      value: Double,
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {

    DoubleMetric(entity, name, instance, Success(value))
  }

  def emptyStateException(analyzer: JdbcAnalyzer[_, _]): EmptyStateException = {
    new EmptyStateException(s"Empty state for analyzer $analyzer, all input values were NULL.")
  }

  def metricFromEmpty(
      analyzer: JdbcAnalyzer[_, _],
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {
    metricFromFailure(emptyStateException(analyzer), name, instance, entity)
  }

  def metricFromFailure(
      exception: Throwable,
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {

    DoubleMetric(entity, name, instance, Failure(
      MetricCalculationException.wrapIfNecessary(exception)))
  }
}
