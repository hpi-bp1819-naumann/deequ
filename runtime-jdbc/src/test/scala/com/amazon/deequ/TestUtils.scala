package com.amazon.deequ

import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.JdbcEngine
import com.amazon.deequ.runtime.jdbc.operators.Table
import com.amazon.deequ.statistics.Statistic

object TestUtils {

  def calculateSingle(statistic: Statistic, table: Table): Metric[_] = {
    JdbcEngine.computeOn(table, Seq(statistic)).metricMap(statistic)
  }
}
