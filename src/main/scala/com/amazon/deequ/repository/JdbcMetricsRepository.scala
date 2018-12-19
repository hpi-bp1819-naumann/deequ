package com.amazon.deequ.repository

import com.amazon.deequ.analyzers.runners.JdbcAnalyzerContext

/**
  * Common trait for RepositoryIndexes where deequ runs can be stored.
  * Repository provides methods to store AnalysisResults(metrics) and VerificationResults(if any)
  */
trait JdbcMetricsRepository {

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey       A ResultKey that uniquely identifies a AnalysisResult
    * @param analyzerContext The resulting AnalyzerContext of an Analysis
    */
  def save(resultKey: ResultKey, analyzerContext: JdbcAnalyzerContext): Unit

  /**
    * Get a AnalyzerContext saved using exactly the same resultKey if present
    */
  def loadByKey(resultKey: ResultKey): Option[JdbcAnalyzerContext]

  /** Get a builder class to construct a loading query to get AnalysisResults */
  def load(): JdbcMetricsRepositoryMultipleResultsLoader

}
