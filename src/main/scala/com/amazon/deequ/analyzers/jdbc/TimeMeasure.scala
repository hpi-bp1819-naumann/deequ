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

package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc
import java.io._
import java.sql.ResultSet

object TimeMeasure extends App {

  var db_size = "0 Mio"

  def run: Unit ={
    for (i <- 0 to 19){
      withJdbc { connection =>
        val query =
          s"""
             | COPY mytable1
             | FROM '/home/pschmidt/sample_data/data_""".stripMargin + i.toString + s""".csv'
             | DELIMITER ';'
             | CSV HEADER;
      """.stripMargin
        print("\n=============================\n")
        print(query)
        print("\n=============================\n\n")


        val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY)

        statement.execute()

        db_size = ((i + 1)*2).toString + " Mio"
        print("New Data added: " + db_size + "\n")
      }

      rtCompleteness
      print("1 of 17\n")
      rtCompliance
      print("2 of 17\n")
      rtCorrelation
      print("3 of 17\n")
      rtCountDistinct
      print("4 of 17\n")
      rtDataType
      print("5 of 17\n")
      rtDistinctness
      print("6 of 17\n")
      rtEntropy
      print("7 of 17\n")
      rtHistogram
      print("8 of 17\n")
      rtMaximum
      print("9 of 17\n")
      rtMean
      print("10 of 17\n")
      rtMinimum
      print("11 of 17\n")
      rtPatternMatch
      print("12 of 17\n")
      rtSize
      print("13 of 17\n")
      rtStandardDeviation
      print("14 of 17\n")
      rtSum
      print("15 of 17\n")
      rtUniqueness
      print("16 of 17\n")
      rtUniqueValueRatio
      print("17 of 17\n")

      print("Finished Measuring")
    }
  }

  def rtCompleteness: Unit = {
    val writer = new FileWriter("runtimeTest/Completeness.csv", true)
    writer.write("\n" + db_size)
    for (x <- 1 to 10) {
      withJdbc { connection =>
        val table = Table("mytable1", connection)
        val t0 = System.currentTimeMillis()
        val result = JdbcCompliance("constraint name", "repeated_integer > 25").calculate(table)
        val t1 = System.currentTimeMillis()
        writer.write(";" + (t1 - t0).toString)
      }
    }
    writer.close()
  }


    def rtCompliance {
      val writer = new FileWriter("runtimeTest/Compliance.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcCompliance("constraint name", "repeated_integer > 25").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtCorrelation {
      val writer = new FileWriter("runtimeTest/Correlation.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcCorrelation("idx", "random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtCountDistinct {
      val writer = new FileWriter("runtimeTest/CountDistinct.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcCountDistinct("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtDataType {
      val writer = new FileWriter("runtimeTest/DataType.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcDataType("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtDistinctness {
      val writer = new FileWriter("runtimeTest/Distinctness.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcDistinctness("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtEntropy {
      val writer = new FileWriter("runtimeTest/Entropy.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcEntropy("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtHistogram {
      val writer = new FileWriter("runtimeTest/Histogram.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcHistogram("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtMaximum {
      val writer = new FileWriter("runtimeTest/Maximum.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcMaximum("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtMean {
      val writer = new FileWriter("runtimeTest/Mean.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcMean("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtMinimum {
      val writer = new FileWriter("runtimeTest/Minimum.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcMinimum("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtPatternMatch {
      val writer = new FileWriter("runtimeTest/PatternMatch.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcPatternMatch("random_integer", raw"[0-9](4-5)".r).calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtSize {
      val writer = new FileWriter("runtimeTest/Size.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcSize().calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtStandardDeviation {
      val writer = new FileWriter("runtimeTest/StandardDeviation.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcStandardDeviation("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtSum {
      val writer = new FileWriter("runtimeTest/Sum.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcSum("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtUniqueness {
      val writer = new FileWriter("runtimeTest/Uniqueness.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcUniqueness("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }

    def rtUniqueValueRatio {
      val writer = new FileWriter("runtimeTest/UniqueValueRatio.csv", true)
      writer.write("\n" + db_size)
      for (x <- 1 to 10) {
        withJdbc { connection =>
          val table = Table("mytable1", connection)
          val t0 = System.currentTimeMillis()
          val result = JdbcUniqueValueRatio("random_integer").calculate(table)
          val t1 = System.currentTimeMillis()
          writer.write(";" + (t1 - t0).toString)
        }
      }
      writer.close()
    }
}
