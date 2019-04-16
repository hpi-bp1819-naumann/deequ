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

package com.amazon.deequ.analyzers

import com.amazon.deequ.JdbcContextSpec
import com.amazon.deequ.runtime.jdbc.JdbcHelpers
import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class StatesTest extends WordSpec with Matchers with JdbcContextSpec with FixtureSupport {

  "FrequenciesAndNumRows" should {
    "merge correctly" in withJdbc { connection =>

      val schema = JdbcStructType(
        JdbcStructField("att1", StringType) :: Nil)

      val dataA = JdbcHelpers.fillTableWithData("dataA", schema, Seq("A", "A", "B").map(Seq(_)), connection)
      val dataB = JdbcHelpers.fillTableWithData("dataB", schema, Seq("A", "C", "C").map(Seq(_)), connection)

      val stateA = FrequencyBasedOperator.computeFrequencies(dataA, "att1" :: Nil)
      val stateB = FrequencyBasedOperator.computeFrequencies(dataB, "att1" :: Nil)

      val stateAB = stateA.sum(stateB)

      println(stateA.table.schema())
      stateA.frequencies()._2.foreach { println }
      println()

      println(stateB.table.schema())
      stateB.frequencies()._2.foreach { println }
      println()

      println(stateAB.table.schema())
      stateAB.frequencies()._2.foreach { println }

      val mergedFrequencies = stateAB.frequencies()._2
          .map(keyValuePair => keyValuePair._1.head -> keyValuePair._2)

      assert(mergedFrequencies.size == 3)
      assert(mergedFrequencies.get("A").contains(3))
      assert(mergedFrequencies.get("B").contains(1))
      assert(mergedFrequencies.get("C").contains(2))
    }
  }
}
