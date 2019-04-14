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

package com.amazon.deequ.jdbc

import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.jdbc.executor.{JdbcExecutor, OperatorResults}
import com.amazon.deequ.runtime.jdbc.operators.{Operator, Table}
import com.amazon.deequ.runtime.jdbc.{JdbcStateLoader, JdbcStatePersister}

/**
  * Defines a set of analyzers to run on data.
  *
  * @param operators
  */
case class OperatorList(operators: Seq[Operator[_, Metric[_]]] = Seq.empty) {

  def addAnalyzer(operator: Operator[_, Metric[_]]): OperatorList = {
    OperatorList(operators :+ operator)
  }

  def addAnalyzers(otherOperators: Seq[Operator[_, Metric[_]]]): OperatorList = {
    OperatorList(operators ++ otherOperators)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @return
    */
  @deprecated("Use the AnalysisRunner instead (the onData method there)", "24-09-2019")
  def run(
           data: Table,
           aggregateWith: Option[JdbcStateLoader] = None,
           saveStatesWith: Option[JdbcStatePersister] = None)
  : OperatorResults = {

    JdbcExecutor.doAnalysisRun(data, operators, aggregateWith = aggregateWith,
      saveStatesWith = saveStatesWith)
  }
}
