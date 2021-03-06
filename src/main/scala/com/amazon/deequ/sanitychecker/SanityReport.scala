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

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.analyzers.CountDistinct
import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfiles, NumericColumnProfile}
import com.google.gson.{GsonBuilder, JsonArray, JsonObject, JsonParser}

/**
  * The result returned from the SanityChecker
  *
  * @param profilingResult Results of column profiling
  * @param verificationResult Results of constraints applied
  * @param sanityCheckerOptions Options specified for the sanity checker run
  */
case class SanityReport(
                         profilingResult: ColumnProfiles,
                         verificationResult: VerificationResult,
                         sanityCheckerOptions: SanityCheckerOptions
                       )

object SanityReport {

  def toJson(report: SanityReport): String = {

    val json = new JsonObject()
    val parser = new JsonParser()

    val profiles = ColumnProfiles.toJsonObject(report.profilingResult.profiles.values.toSeq)
    val checkResults =
      parser.parse(VerificationResult.checkResultsAsJson(report.verificationResult))
    val metrics = parser.parse(VerificationResult.successMetricsAsJson(report.verificationResult))
    val options = optionsToJsonObject(report.sanityCheckerOptions)

    json.add("profiles", profiles)
    json.add("checkResults", checkResults)
    json.add("metrics", metrics)
    json.add("options", options)

    val gson = new GsonBuilder()
      .setPrettyPrinting()
      .create()

    gson.toJson(json)
  }

  def optionsToJsonObject(sanityCheckerOptions: SanityCheckerOptions): JsonObject = {

    val options = new JsonObject()
    if (sanityCheckerOptions.label.nonEmpty) {
      options.addProperty("label", sanityCheckerOptions.label.get)
    }
    options.addProperty(
      "featureCompleteness", sanityCheckerOptions.featureCompleteness)
    if (sanityCheckerOptions.exactDistinctCountForColumns.nonEmpty) {
      options.addProperty(
        "exactDistinctCountForColumns",
        sanityCheckerOptions.exactDistinctCountForColumns.get.mkString(", "))
    }
    if (sanityCheckerOptions.columnWhitelists.nonEmpty) {
      val whitelistJson = new JsonObject()
      sanityCheckerOptions.columnWhitelists.getOrElse(Seq()).foreach {
        case (column, whitelist) =>
          whitelistJson.addProperty(column, whitelist.mkString(", "))
      }
      options.add("columnWhitelists", whitelistJson)
    }
    if (sanityCheckerOptions.columnBlacklists.nonEmpty) {
      val blacklistJson = new JsonObject()
      sanityCheckerOptions.columnBlacklists.getOrElse(Seq()).foreach {
        case (column, blacklist) =>
          blacklistJson.addProperty(column, blacklist.mkString(", "))
      }
      options.add("columnBlacklists", blacklistJson)
    }
    options
  }

  def print(report: SanityReport): Unit = {
    // step 3: output profiles
    val distinctCountResults = report.verificationResult.metrics.collect {
      case (analyzer: CountDistinct, metric: DoubleMetric) =>
        analyzer.columns -> metric.value.get.toInt
    }
    report.profilingResult.profiles
      .foreach { case (name, profile) =>

        printGenericColumnProfile(name, profile, distinctCountResults)
        profile match {
          case columnProfile: NumericColumnProfile =>
            printNumericStatistics(name, columnProfile)
          case _ =>
            printFullDistribution(name, profile)
        }
      }

    // step 4: report failures
    if (report.verificationResult.status != Success) {
      val resultsForAllConstraints = report.verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      if(report.sanityCheckerOptions.label.isDefined) {
        reportLabelResult(report.sanityCheckerOptions.label, resultsForAllConstraints)
      }
      reportFeatureResults(report.sanityCheckerOptions.label, resultsForAllConstraints)
      reportValueAllowanceResults(resultsForAllConstraints)
    }
  }

  private[this] def reportLabelResult(
                                       label: Option[String],
                                       results: Iterable[ConstraintResult])
  : Unit = {
    results
      .filter { result => label.contains(result.metric.get.instance)}
      .foreach { result => result.status match {
        case ConstraintStatus.Success =>
          println(s"All rows contain a label in '${result.metric.get.instance}' " +
            s"and may be used as training or test data. " +
            s"Completeness check successful: ${result.message.get}")
        case _ =>
          println(s"Some rows are missing a label in '${result.metric.get.instance}' " +
            s"and can't be used as training or test data. " +
            s"Completeness check failed: ${result.message.get}")
      }
      }
  }

  private[this] def reportFeatureResults(
                                          label: Option[String],
                                          results: Iterable[ConstraintResult])
  : Unit = {
    results
      .filter { result => result.status != ConstraintStatus.Success &&
        result.constraint.toString.contains("Completeness") &&
        !label.contains(result.metric.get.instance)}
      .foreach { result =>
        println(s"The feature '${result.metric.get.instance}' " +
          s"may not contain enough values to train a model. " +
          s"Completeness check failed: ${result.message.get}")
      }
  }

  private[this] def reportValueAllowanceResults(results: Iterable[ConstraintResult]): Unit = {
    results
      .filter { result => result.status != ConstraintStatus.Success &&
        (result.metric.get.instance.endsWith(" whitelist") ||
         result.metric.get.instance.endsWith(" blacklist") )}
      .foreach { result =>
        println(s"The ${result.metric.get.instance} is violated: ${result.message.get}")
      }
  }

  private[this] def printGenericColumnProfile(
                                               name: String,
                                               profile: ColumnProfile,
                                               distinctCountResults: Map[Seq[String], Int])
  : Unit = {
    val distinctOutput = if (distinctCountResults.contains(Seq(name))) {
      s"number of distinct values: ${distinctCountResults(Seq(name))}"
    } else {
      s"approximate number of distinct values: ${profile.approximateNumDistinctValues}"
    }
    println(s"Column '$name':\n " +
      s"\tcompleteness: ${profile.completeness}\n" +
      s"\t$distinctOutput\n" +
      s"\tdatatype: ${profile.dataType}\n")
  }

  private[this] def printNumericStatistics(
                                            name: String,
                                            profile: NumericColumnProfile): Unit = {
    println(s"Statistics of '$name':\n" +
      s"\tminimum: ${profile.minimum.get}\n" +
      s"\tmaximum: ${profile.maximum.get}\n" +
      s"\tmean: ${profile.mean.get}\n" +
      s"\tstandard deviation: ${profile.stdDev.get}\n")
    println()
  }

  private[this] def printFullDistribution(
                                    name: String,
                                    profile: ColumnProfile): Unit = {
    if (profile.histogram.isDefined) {
      println(s"Value distribution in '$name':")
      profile.histogram.foreach {
        _.values.foreach { case (key, entry) =>
          println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
        }
      }
      println(s"\n")
    }
  }
}
