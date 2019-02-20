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

import org.apache.spark.sql.DataFrame

/** A class to build a Constraint Suggestion run using a fluent API */
class SanityCheckerRunBuilder(val data: DataFrame) {

  protected var label: Option[String] = None
  protected var featureCompleteness: Double =
    SanityChecker.DEFAULT_FEATURE_COMPLETENESS

  protected var printStatusUpdates: Boolean = false
  protected var cacheInputs: Boolean = false
  protected var restrictToColumns: Option[Seq[String]] = None
  protected var exactDistinctCountForColumns: Option[Seq[String]] = None
  protected var columnWhitelists: Option[Map[String, Seq[String]]] = None
  protected var columnBlacklists: Option[Map[String, Seq[String]]] = None

  protected def this(constraintSuggestionRunBuilder: SanityCheckerRunBuilder) {

    this(constraintSuggestionRunBuilder.data)

    printStatusUpdates = constraintSuggestionRunBuilder.printStatusUpdates
    cacheInputs = constraintSuggestionRunBuilder.cacheInputs
    restrictToColumns = constraintSuggestionRunBuilder.restrictToColumns
  }

  /**
    * Print status updates between passes
    *
    * @param printStatusUpdates Whether to print status updates
    */
  def printStatusUpdates(printStatusUpdates: Boolean): this.type = {
    this.printStatusUpdates = printStatusUpdates
    this
  }

  /**
    * Cache inputs
    *
    * @param cacheInputs Whether to cache inputs
    */
  def cacheInputs(cacheInputs: Boolean): this.type = {
    this.cacheInputs = cacheInputs
    this
  }

  /**
    * Can be used to specify a subset of columns to look at
    *
    * @param restrictToColumns The columns to look at
    */
  def restrictToColumns(restrictToColumns: Seq[String]): this.type = {
    this.restrictToColumns = Option(restrictToColumns)
    this
  }

  /**
    * Can be used to specify a column which shall be used as label for machine learning.
    * Corresponding checks are performed.
    *
    * @param label The column to treat as label
    */
  def withLabel(label: String): this.type = {
    this.label = Some(label)
    this
  }

  /**
    * Can be used to specify a level of completeness to verify for columns.
    *
    * @param featureCompleteness The level of completeness to verify
    */
  def withFeatureCompleteness(featureCompleteness: Double): this.type = {
    this.featureCompleteness = featureCompleteness
    this
  }

  /**
    * Can be used to specify a subset of columns for which the exact count of distinct values
    * should be determined. Slow for diverse columns.
    *
    * @param exactDistinctCountForColumns The columns to count distinct values for
    */
  def withExactDistinctCountForColumns(exactDistinctCountForColumns: Seq[String]): this.type = {
    this.exactDistinctCountForColumns = Some(exactDistinctCountForColumns)
    this
  }

  /**
    * Can be used to specify a subset of columns and their expected set of values
    *
    * @param columnWhitelists Mapping of columns to sets of values
    */
  def withColumnWhitelists(columnWhitelists: Map[String, Seq[String]]): this.type = {
    this.columnWhitelists = Some(columnWhitelists)
    this
  }

  /**
    * Can be used to specify a subset of columns and their prohibited values
    *
    * @param columnBlacklists Mapping of columns to sets of values
    */
  def withColumnBlacklists(columnBlacklists: Map[String, Seq[String]]): this.type = {
    this.columnBlacklists = Some(columnBlacklists)
    this
  }

  def run(): SanityReport = {
   SanityCheckerRunner().run(
      data,
      label,
      featureCompleteness,
      restrictToColumns,
      printStatusUpdates,
      exactDistinctCountForColumns,
      columnWhitelists,
      columnBlacklists
    )
  }
}
