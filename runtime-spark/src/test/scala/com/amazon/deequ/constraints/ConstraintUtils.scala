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

package com.amazon.deequ.constraints

import com.amazon.deequ.runtime.spark.SparkEngine
import org.apache.spark.sql.DataFrame

object ConstraintUtils {

  def calculate(constraint: Constraint, df: DataFrame): ConstraintResult = {

    val statistic = constraint.asInstanceOf[StatisticConstraint[_,_]].statistic

    val computed = SparkEngine.computeOn(df, Seq(statistic))

    constraint.evaluate(computed.metricMap)
  }
}
