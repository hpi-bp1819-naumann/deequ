package com.amazon.deequ.analyzers

import java.sql.{Connection, ResultSet}

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import com.amazon.deequ.analyzers.jdbc.{JdbcFrequencyBasedAnalyzerUtils, Table}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

trait FrequenciesAndNumRows extends State[FrequenciesAndNumRows] {

  def numRows: Long

  override def sum(other: FrequenciesAndNumRows): FrequenciesAndNumRows
}

/** State representing frequencies of groups in the data, as well as overall #rows */
case class FrequenciesAndNumRowsWithSpark(frequencies: DataFrame, numRows: Long)
  extends FrequenciesAndNumRows {

  /** Add up frequencies via an outer-join */
  override def sum(other: FrequenciesAndNumRows): FrequenciesAndNumRowsWithSpark = {

    val otherState = other.asInstanceOf[FrequenciesAndNumRowsWithSpark]

    val columns = frequencies.schema.fields
      .map { _.name }
      .filterNot { _ == COUNT_COL }

    val projectionAfterMerge =
      columns.map { column => coalesce(col(s"this.$column"), col(s"other.$column")).as(column) } ++
        Seq((zeroIfNull(s"this.$COUNT_COL") + zeroIfNull(s"other.$COUNT_COL")).as(COUNT_COL))

    /* Null-safe join condition over equality on grouping columns */
    val joinCondition = columns.tail
      .foldLeft(nullSafeEq(columns.head)) { case (expr, column) => expr.and(nullSafeEq(column)) }

    /* Null-safe outer join to merge histograms */
    val frequenciesSum = frequencies.alias("this")
      .join(otherState.frequencies.alias("other"), joinCondition, "outer")
      .select(projectionAfterMerge: _*)

    FrequenciesAndNumRowsWithSpark(frequenciesSum, numRows + otherState.numRows)
  }

  private[analyzers] def nullSafeEq(column: String): Column = {
    col(s"this.$column") <=> col(s"other.$column")
  }

  private[analyzers] def zeroIfNull(column: String): Column = {
    coalesce(col(column), lit(0))
  }
}


case class FrequenciesAndNumRowsWithJdbc(table: Table,
                                         private var _numRows: Option[Long] = None,
                                         private var _numNulls: Option[Long] = None)
  extends FrequenciesAndNumRows {


  def numNulls(): Long = {
    _numNulls match {
      case None =>
        val firstGroupingColumn = table.columns().head._1
        val numNulls = s"SUM(CASE WHEN ($firstGroupingColumn = NULL) THEN absolute ELSE 0 END)"

        val result = table.executeAggregations(numNulls :: Nil)

        _numNulls = Some(result.getLong(0))
      case Some(_) =>
    }

    _numNulls.get
  }

  def numRows(): Long = {
    _numRows match {
      case None =>
        val numRows = s"SUM(absolute)"

        val result = table.executeAggregations(numRows :: Nil)

        _numRows = Some(result.getLong(0))
      case Some(_) =>
    }

    _numRows.get
  }

  def frequencies(): (mutable.LinkedHashMap[String, String], Map[Seq[String], Long]) = {

    table.withJdbc[(mutable.LinkedHashMap[String, String], Map[Seq[String], Long])] {
      connection: Connection =>

        var frequencies = Map[Seq[String], Long]()

        val query =
          s"""
             |SELECT
             | *
             |FROM
             | ${table.name}
      """.stripMargin

        val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY)

        val result = statement.executeQuery()
        val numGroupingColumns = result.getMetaData.getColumnCount - 1

        while (result.next()) {
          val columns = (1 to numGroupingColumns).map(col => result.getString(col)).seq
          frequencies += (columns -> result.getLong(numGroupingColumns + 1))
        }

        (table.columns(), frequencies)
    }
  }

  override def sum(other: FrequenciesAndNumRows): FrequenciesAndNumRowsWithJdbc = {

    val otherState = other.asInstanceOf[FrequenciesAndNumRowsWithJdbc]

    val totalRows = numRows + otherState.numRows
    val newTable = JdbcFrequencyBasedAnalyzerUtils.join(table, otherState.table)

    FrequenciesAndNumRowsWithJdbc(newTable, Some(totalRows), Some(numNulls() + otherState.numNulls()))
  }
}


object FrequenciesAndNumRowsWithJdbc {

  def from(columns: mutable.LinkedHashMap[String, String],
           frequencies: Map[Seq[String], Long], numRows: Long): FrequenciesAndNumRowsWithJdbc = {

    val table = Table.createAndFill(JdbcFrequencyBasedAnalyzerUtils.newDefaultTable(), columns, frequencies)

    FrequenciesAndNumRowsWithJdbc(table, Some(numRows))
  }
}
