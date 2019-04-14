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
package analyzers

import java.sql.Connection

import com.amazon.deequ.runtime.jdbc.operators.JdbcColumn._
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.jdbc.{FileSystemJdbcStateProvider, InMemoryJdbcStateProvider, JdbcStateLoader, JdbcStatePersister}
import com.amazon.deequ.statistics.Patterns
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

class StateProviderTest extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  "Operators" should {

    "correctly restore their state from memory" in withJdbc { connection =>

      val provider = InMemoryJdbcStateProvider()

      val data = someData(connection)

      assertCorrectlyRestoresState[NumMatches](provider, provider, SizeOp(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        CompletenessOp("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        ComplianceOp("att1", "att1 = 'b'"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        PatternMatchOp("att1", Patterns.EMAIL), data)

      assertCorrectlyRestoresState[SumState](provider, provider, SumOp("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, MeanOp("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, MinimumOp("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, MaximumOp("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        StandardDeviationOp("price"), data)

      assertCorrectlyRestoresState[DataTypeHistogram](provider, provider, DataTypeOp("item"), data)
      //assertCorrectlyRestoresStateForHLL(provider, provider, ApproxCountDistinctOp("att1"), data)
      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        CorrelationOp("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, UniquenessOp("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        UniquenessOp(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, EntropyOp("att1"), data)

      //assertCorrectlyApproxQuantileState(provider, provider, ApproxQuantileOp("price", 0.5), data)
    }

    "correctly restore their state from the filesystem" in withJdbc { connection =>

      val tempDir = TempFileUtils.tempDir("stateRestoration")

      val provider = FileSystemJdbcStateProvider(tempDir)

      val data = someData(connection)

      assertCorrectlyRestoresState[NumMatches](provider, provider, SizeOp(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        CompletenessOp("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        ComplianceOp("att1", "att1 = 'b'"), data)

      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        PatternMatchOp("att1", Patterns.EMAIL), data)

      assertCorrectlyRestoresState[SumState](provider, provider, SumOp("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, MeanOp("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, MinimumOp("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, MaximumOp("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        StandardDeviationOp("price"), data)

      assertCorrectlyRestoresState[DataTypeHistogram](provider, provider, DataTypeOp("item"), data)
      //assertCorrectlyRestoresStateForHLL(provider, provider, ApproxCountDistinctOp("att1"), data)
      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        CorrelationOp("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, UniquenessOp("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        UniquenessOp(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, EntropyOp("att1"), data)

      //assertCorrectlyApproxQuantileState(provider, provider, ApproxQuantileOp("price", 0.5), data)
    }
  }

  def assertCorrectlyRestoresState[S <: State[S]](
      persister: JdbcStatePersister,
      loader: JdbcStateLoader,
      analyzer: Operator[S, _],
      data: Table) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[S](analyzer, state)
    val clonedState = loader.load[S](analyzer)

    assert(clonedState.isDefined)
    assert(state == clonedState.get)
  }

  /* def assertCorrectlyApproxQuantileState(
      persister: JdbcStatePersister,
      loader: JdbcStateLoader,
      analyzer: Operator[ApproxQuantileState, _],
      data: Table) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)

    val state = stateResult.get

    persister.persist[ApproxQuantileState](analyzer, state)
    val clonedState = loader.load[ApproxQuantileState](analyzer)

    assert(clonedState.isDefined)
    val summary = state.percentileDigest.quantileSummaries
    val clonedSummary = clonedState.get.percentileDigest.quantileSummaries

    assert(summary.compressThreshold == clonedSummary.compressThreshold)
    assert(summary.relativeError == clonedSummary.relativeError)
    assert(summary.count == clonedSummary.count)
    assert(summary.sampled.sameElements(clonedSummary.sampled))
  }

  def assertCorrectlyRestoresStateForHLL(
      persister: JdbcStatePersister,
      loader: JdbcStateLoader,
      analyzer: Operator[ApproxCountDistinctState, _],
      data: Table) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[ApproxCountDistinctState](analyzer, state)
    val clonedState = loader.load[ApproxCountDistinctState](analyzer)

    assert(clonedState.isDefined)
    assert(state.words.sameElements(clonedState.get.words))
  } */

  def assertCorrectlyRestoresFrequencyBasedState(
      persister: JdbcStatePersister,
      loader: JdbcStateLoader,
      analyzer: Operator[FrequenciesAndNumRows, _],
      data: Table) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[FrequenciesAndNumRows](analyzer, state)
    val clonedState = loader.load[FrequenciesAndNumRows](analyzer, Some(data.jdbcConnection))

    assert(clonedState.isDefined)
    assert(state.numRows == clonedState.get.numRows)
    assert(state.frequencies().toString() == clonedState.get.frequencies().toString())
  }

  def someData(connection: Connection): Table = {

    val schema = JdbcStructType(
        JdbcStructField("item", StringType) ::
          JdbcStructField("att1", StringType) ::
          JdbcStructField("count", IntegerType) ::
          JdbcStructField("price", FloatType) :: Nil)

    val data =
      Seq(
        Seq("1", "a", 17, 1.3),
        Seq("2", null, 12, 76.0),
        Seq("3", "b", 15, 89.0),
        Seq("4", "b", 12, 12.7),
        Seq("5", null, 1, 1.0),
        Seq("6", "a", 21, 78.0),
        Seq("7", null, 12, 0.0))

    fillTableWithData("someData", schema, data, connection)
  }

}
