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

package com.amazon.deequ.sanitychecker

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.CountDistinct
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.profiles.ColumnProfilerRunner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/** Performs basic sanity checks on a dataset intended for training a model.
  *
  * In the first step, the column profiler is run for every column. An exact distinct count can be
  * requested on demand.
  *
  * In the second step, the verification suite is run with customizable completeness checks on
  * feature and label columns. Additional compliance checks for certain whitelisted or blacklisted
  * values may also be added.
  *
  * A SanityReport containing the customized parameters, the profiling result and the verification
  * result is returned.
  *
  */
object SanityChecker {

  val DEFAULT_FEATURE_COMPLETENESS = 1.0

  private[deequ] def check(
      data: DataFrame,
      label: Option[String],
      featureCompleteness: Double =
        SanityChecker.DEFAULT_FEATURE_COMPLETENESS,
      restrictToColumns: Option[Seq[String]] = None,
      printStatusUpdates: Boolean = false,
      exactDistinctCountForColumns: Option[Seq[String]],
      columnWhitelists: Option[Map[String, Seq[String]]],
      columnBlacklists: Option[Map[String, Seq[String]]])
    : SanityReport = {

    // step 1: profile columns
    val profilingResult = ColumnProfilerRunner()
      .onData(data)
      .printStatusUpdates(printStatusUpdates)
      .run()

    // step 2: apply constraints
    var verification = VerificationSuite()
      .onData(data)
      .addCheck(
        getFeatureCompletenessCheck(
          label,
          getRelevantColumns(data.schema, label, restrictToColumns),
          featureCompleteness))
    if(exactDistinctCountForColumns.isDefined) {
      verification = exactDistinctCountForColumns
        .foldLeft(verification) { (suite, column) =>
          suite.addRequiredAnalyzer(CountDistinct(column))
        }
    }
    if(columnWhitelists.isDefined) {
      verification = verification.addCheck(
        getValueAllowanceCheck(allowed = true, columnWhitelists.get)
      )
    }
    if(columnBlacklists.isDefined) {
      verification = verification.addCheck(
        getValueAllowanceCheck(allowed = false, columnBlacklists.get)
      )
    }
    val verificationResult = verification.run()

    SanityReport(profilingResult, verificationResult, label, exactDistinctCountForColumns,
      columnWhitelists, columnBlacklists)
  }

  private[this] def getFeatureCompletenessCheck(
      label: Option[String],
      features: Seq[String],
      featureCompleteness: Double)
  : Check = {
    val completenessCheck = label.foldLeft(Check(CheckLevel.Warning, "completeness checks")) {
      (check, label) => check.isComplete(label)
    }
    features.foldLeft(completenessCheck) { (check, feature) =>
      check.hasCompleteness(feature, _ >= featureCompleteness)
    }
  }

  private[this] def getValueAllowanceCheck(
      allowed: Boolean,
      valueLists: Map[String, Seq[String]])
  : Check = {
    if (allowed) {
      valueLists.foldLeft(Check(CheckLevel.Warning, "whitelist checks")) {
        (check, list) =>
          val allowedValues = list._2.mkString("'", "', '", "'")
          check.addConstraint(
            Constraint.complianceConstraint(
              s"${list._1} whitelist",
              s"${list._1} IN ($allowedValues)",
              _ == 1.0
            )
          )
      }
    } else {
      valueLists.foldLeft(Check(CheckLevel.Warning, "blacklist checks")) {
        (check, list) =>
          val forbiddenValues = list._2.mkString("'", "', '", "'")
          check.addConstraint(
            Constraint.complianceConstraint(
              s"${list._1} blacklist",
              s"${list._1} NOT IN ($forbiddenValues)",
              _ == 1.0
            )
          )
      }
    }
  }

  private[this] def getRelevantColumns(
      schema: StructType,
      label: Option[String],
      restrictToColumns: Option[Seq[String]])
  : Seq[String] = {

    schema.fields
      .filter { field => label.isEmpty || !label.contains(field.name) }
      .filter { field => restrictToColumns.isEmpty || restrictToColumns.get.contains(field.name) }
      .map { field => field.name }
  }
}
