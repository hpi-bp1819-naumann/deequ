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

import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.JdbcEngine
import com.amazon.deequ.runtime.jdbc.operators.Table
import com.amazon.deequ.statistics.Statistic

object TestUtils {

  def calculateSingle(statistic: Statistic, table: Table): Metric[_] = {
    JdbcEngine.computeOn(table, Seq(statistic)).metricMap(statistic)
  }
}
