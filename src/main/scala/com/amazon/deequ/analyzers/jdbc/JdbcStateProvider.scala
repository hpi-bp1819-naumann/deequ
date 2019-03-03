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

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

import com.amazon.deequ.analyzers._
import com.amazon.deequ.io.LocalDiskUtils
import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.avro.generic.GenericData
import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession, types}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

private object StateInformation {
  // 4 byte for the id of an DataTypeInstances entry + 8 byte for the (Long) count of that data type
  // as used in HdfsStateProvider.persistDataTypeState and HdfsStateProvider.loadDataTypeState
  final val dataTypeCountEntrySizeInBytes = 4 + 8
}

/** Load a stored state for an analyzer */
trait JdbcStateLoader {
  def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S]
}

/** Persist a state for an analyzer */
trait JdbcStatePersister {
  def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S)
}

/** Store states in memory */
case class JdbcInMemoryStateProvider(connection: Connection)
  extends JdbcStateLoader with JdbcStatePersister {

  private[this] val statesByAnalyzer = new ConcurrentHashMap[JdbcAnalyzer[_, _], State[_]]()

  override def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S] = {
    Option(statesByAnalyzer.get(analyzer).asInstanceOf[S])
  }

  override def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S): Unit = {
    statesByAnalyzer.put(analyzer, state)
  }

  override def toString: String = {
    val buffer = new StringBuilder()
    statesByAnalyzer.foreach { case (analyzer, state) =>
      buffer.append(analyzer.toString)
      buffer.append(" => ")
      buffer.append(state.toString)
      buffer.append("\n")
    }

    buffer.toString
  }
}

/** Store states on a filesystem (supports local disk) */
case class JdbcFileSystemStateProvider(
    locationPrefix: String,
    numPartitionsForHistogram: Int = 10,
    allowOverwrite: Boolean = false,
    connection: Connection)
  extends JdbcStateLoader with JdbcStatePersister {

  private[this] def toIdentifier[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): String = {
    MurmurHash3.stringHash(analyzer.toString, 42).toString
  }

  override def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S): Unit = {

    val identifier = toIdentifier(analyzer)

    analyzer match {
      case _: JdbcSize =>
        persistLongState(state.asInstanceOf[NumMatches].numMatches, identifier)

      case _ : JdbcCompleteness | _ : JdbcCompliance =>
        persistLongLongState(state.asInstanceOf[NumMatchesAndCount], identifier)

      case _: JdbcSum =>
        persistDoubleState(state.asInstanceOf[SumState].sum, identifier)

      case _: JdbcMean =>
        persistDoubleLongState(state.asInstanceOf[MeanState], identifier)

      case _: JdbcMinimum =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _: JdbcMaximum =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _: JdbcHistogram =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _: JdbcEntropy =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _ : JdbcStandardDeviation =>
        persistStandardDeviationState(state.asInstanceOf[StandardDeviationState], identifier)

      case _ : JdbcCorrelation =>
        persistCorrelationState(state.asInstanceOf[CorrelationState], identifier)

      case _ : JdbcUniqueness =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _ =>
        throw new IllegalArgumentException(s"Unable to persist state for analyzer $analyzer.")
    }
  }

  override def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S] = {

    val identifier = toIdentifier(analyzer)

    val state: Any = analyzer match {

      case _ : JdbcSize => NumMatches(loadLongState(identifier))

      case _ : JdbcCompleteness | _ : JdbcCompliance => loadLongLongState(identifier)

      case _ : JdbcSum => SumState(loadDoubleState(identifier))

      case _ : JdbcMean => loadDoubleLongState(identifier)

      case _ : JdbcMinimum => MinState(loadDoubleState(identifier))

      case _ : JdbcMaximum => MaxState(loadDoubleState(identifier))

      case _ : JdbcHistogram => loadFrequenciesLongState(identifier, connection)

      case _ : JdbcEntropy => loadFrequenciesLongState(identifier, connection)

      case _ : JdbcStandardDeviation => loadStandardDeviationState(identifier)

      case _ : JdbcCorrelation => loadCorrelationState(identifier)

      case _ : JdbcUniqueness => loadFrequenciesLongState(identifier, connection)

      case _ =>
        throw new IllegalArgumentException(s"Unable to load state for analyzer $analyzer.")
    }

    Option(state.asInstanceOf[S])
  }

  private[this] def persistLongState(state: Long, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeLong(state)
    }
  }

  private[this] def persistDoubleState(state: Double, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeDouble(state)
    }
  }

  private[this] def persistLongLongState(state: NumMatchesAndCount, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeLong(state.numMatches)
      out.writeLong(state.count)
    }
  }

  private[this] def persistDoubleLongState(state: MeanState, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.sum)
      out.writeLong(state.count)
    }
  }

  private[this] def persistBytes(bytes: Array[Byte], identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeInt(bytes.length)
      for (index <- bytes.indices) {
        out.writeByte(bytes(index))
      }
    }
  }

  private[this] def persistFrequenciesLongState(
      state: JdbcFrequenciesAndNumRows,
      identifier: String)
    : Unit = {

    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>

      val (cols, data) = state.columnsAndFrequencies()

      out.writeInt(cols.size)

      for ((colName: String, colDataType: String) <- cols) {
        out.writeInt(colName.length)
        for (char <- colName) {
          out.writeChar(char)
        }

        out.writeInt(colDataType.length)
        for (char <- colDataType) {
          out.writeChar(char)
        }
      }

      out.writeLong(data.size)

      for ((key: Seq[String], value: Long) <- data) {
        out.writeInt(key.size)
        for (str <- key) {
          val string = str match {
            case null => "deequ__nullString"
            case _ => str
          }
          out.writeInt(string.length)
          for (char <- string) {
            out.writeChar(char)
          }
        }
        out.writeLong(value)
      }
      out.writeLong(state.numRows)
    }

  }

  private[this] def persistCorrelationState(state: CorrelationState, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.xAvg)
      out.writeDouble(state.yAvg)
      out.writeDouble(state.ck)
      out.writeDouble(state.xMk)
      out.writeDouble(state.yMk)
    }
  }

  private[this] def persistStandardDeviationState(
      state: StandardDeviationState,
      identifier: String) {

    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.avg)
      out.writeDouble(state.m2)
    }
  }

  private[this] def loadLongState(identifier: String): Long = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") {
      _.readLong()
    }
  }

  private[this] def loadDoubleState(identifier: String): Double = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") {
      _.readDouble()
    }
  }
  private[this] def loadLongLongState(identifier: String): NumMatchesAndCount = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      NumMatchesAndCount(in.readLong(), in.readLong())
    }
  }

  private[this] def loadDoubleLongState(identifier: String): MeanState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      MeanState(in.readDouble(), in.readLong())
    }
  }

  private[this] def loadBytes(identifier: String): Array[Byte] = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      val length = in.readInt()
      Array.fill(length) { in.readByte() }
    }
  }

  private[this] def loadFrequenciesLongState(identifier: String, connection: Connection):
  JdbcFrequenciesAndNumRows = {

    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>

      val numberOfCols = in.readInt()

      val columns = mutable.LinkedHashMap[String, String]()

      for (_ <- 1 to numberOfCols) {
        val colNameLength = in.readInt()
        val colName: String = (for (_ <- 1 to colNameLength) yield in.readChar()).mkString

        val colDataTypeLength = in.readInt()
        val colDataType: String = (for (_ <- 1 to colDataTypeLength) yield in.readChar()).mkString

        columns(colName) = colDataType
      }

      val numberOfBins = in.readLong()

      def readKeyValuePair(map: Map[Seq[String], Long]): Map[Seq[String], Long] = {
        if (map.size < numberOfBins) {
          val columnsAmount = in.readInt
          var columns = Seq[String]()
          for (_ <- 1 to columnsAmount) {
            val keyLength = in.readInt()
            val str: String = (for (_ <- 1 to keyLength) yield in.readChar()).mkString

            val key = str match {
              case "deequ__nullString" => null
              case _ => str
            }

            columns = columns :+ key
          }
          val value: Long = in.readLong()
          val pair = columns -> value
          readKeyValuePair(map + pair)
        } else {
          map
        }
      }

      val frequencies = readKeyValuePair(Map[Seq[String], Long]())
      val numRows = in.readLong()
      JdbcFrequenciesAndNumRows.from(connection, columns, frequencies, numRows)
    }
  }

  private[this] def loadCorrelationState(identifier: String): CorrelationState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      CorrelationState(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(),
        in.readDouble(), in.readDouble())
    }
  }

  private[this] def loadStandardDeviationState(identifier: String): StandardDeviationState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      StandardDeviationState(in.readDouble(), in.readDouble(), in.readDouble())
    }
  }
}


/** Store states on a filesystem (supports local disk, HDFS, S3) */
case class JdbcHdfsStateProvider(
                              session: SparkSession,
                              connection: Connection,
                              locationPrefix: String,
                              numPartitionsForHistogram: Int = 10,
                              allowOverwrite: Boolean = false)
  extends JdbcStateLoader with JdbcStatePersister {

  import com.amazon.deequ.io.DfsUtils._

  private[this] def toIdentifier[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): String = {
    MurmurHash3.stringHash(analyzer.toString, 42).toString
  }

  override def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S): Unit = {

    val identifier = toIdentifier(analyzer)

    analyzer match {
      case _: JdbcSize =>
        persistLongState(state.asInstanceOf[NumMatches].numMatches, identifier)

      case _ : JdbcCompleteness | _ : JdbcCompliance | _ : JdbcPatternMatch =>
        persistLongLongState(state.asInstanceOf[NumMatchesAndCount], identifier)

      case _: JdbcSum =>
        persistDoubleState(state.asInstanceOf[SumState].sum, identifier)

      case _: JdbcMean =>
        persistDoubleLongState(state.asInstanceOf[MeanState], identifier)

      case _: JdbcMinimum =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _: JdbcMaximum =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _ : JdbcFrequencyBasedAnalyzer | _ : JdbcHistogram =>
        persistDataframeLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _: JdbcDataType =>
        val histogram = state.asInstanceOf[DataTypeHistogram]
        persistBytes(DataTypeHistogram.toBytes(histogram.numNull, histogram.numFractional,
          histogram.numIntegral, histogram.numBoolean, histogram.numString), identifier)
      /*
      case _: ApproxCountDistinct =>
        val counters = state.asInstanceOf[ApproxCountDistinctState]
        persistBytes(DeequHyperLogLogPlusPlusUtils.wordsToBytes(counters.words), identifier)
        */

      case _ : JdbcCorrelation =>
        persistCorrelationState(state.asInstanceOf[CorrelationState], identifier)

      case _ : JdbcStandardDeviation =>
        persistStandardDeviationState(state.asInstanceOf[StandardDeviationState], identifier)

        /*
      case _: ApproxQuantile =>
        val percentileDigest = state.asInstanceOf[ApproxQuantileState].percentileDigest
        val serializedDigest = ApproximatePercentile.serializer.serialize(percentileDigest)
        persistBytes(serializedDigest, identifier)
        */

      case _ =>
        throw new IllegalArgumentException(s"Unable to persist state for analyzer $analyzer.")
    }
  }

  override def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S] = {

    val identifier = toIdentifier(analyzer)

    val state: Any = analyzer match {

      case _ : JdbcSize => NumMatches(loadLongState(identifier))

      case _ : JdbcCompleteness | _ : JdbcCompliance | _ : JdbcPatternMatch => loadLongLongState(identifier)

      case _ : JdbcSum => SumState(loadDoubleState(identifier))

      case _ : JdbcMean => loadDoubleLongState(identifier)

      case _ : JdbcMinimum => MinState(loadDoubleState(identifier))

      case _ : JdbcMaximum => MaxState(loadDoubleState(identifier))

      case _ : JdbcFrequencyBasedAnalyzer | _ : JdbcHistogram => loadDataframeLongState(identifier)

      case _ : JdbcDataType => DataTypeHistogram.fromBytes(loadBytes(identifier))

        /*
      case _ : ApproxCountDistinct =>
        DeequHyperLogLogPlusPlusUtils.wordsFromBytes(loadBytes(identifier))
      */

      case _ : JdbcCorrelation => loadCorrelationState(identifier)

      case _ : JdbcStandardDeviation => loadStandardDeviationState(identifier)

        /*
      case _: ApproxQuantile =>
        val percentileDigest = ApproximatePercentile.serializer.deserialize(loadBytes(identifier))
        ApproxQuantileState(percentileDigest)
        */

      case _ =>
        throw new IllegalArgumentException(s"Unable to load state for analyzer $analyzer.")
    }

    Option(state.asInstanceOf[S])
  }

  private[this] def persistLongState(state: Long, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeLong(state)
    }
  }

  private[this] def persistDoubleState(state: Double, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeDouble(state)
    }
  }

  private[this] def persistLongLongState(state: NumMatchesAndCount, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeLong(state.numMatches)
      out.writeLong(state.count)
    }
  }

  private[this] def persistDoubleLongState(state: MeanState, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.sum)
      out.writeLong(state.count)
    }
  }

  private[this] def persistBytes(bytes: Array[Byte], identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeInt(bytes.length)
      for (index <- bytes.indices) {
        out.writeByte(bytes(index))
      }
    }
  }

  private[this] def persistDataframeLongState(
                                               state: JdbcFrequenciesAndNumRows,
                                               identifier: String)
  : Unit = {

    def toSparkType(sqlType: String): org.apache.spark.sql.types.DataType = {
      println(sqlType)

      sqlType.toUpperCase() match {
        case "BOOLEAN" => BooleanType
        case "CHAR" | "VARCHAR" | "TEXT" => StringType
        case "SMALLINT" | "INTEGER" | "INT4" => IntegerType
        case "INT4" | "BIGINT" => LongType
        case "FLOAT" => FloatType
        case "REAL" | "FLOAT8" | "DOUBLE PRECISION" | "DOUBLE" | "NUMERIC" => DoubleType
        case "DATE" => DateType
        case "TIMESTAMP" => TimestampType

        case _ => throw new Exception(
          s"Could not find a conversion from SQL DataType to Spark DataType for $sqlType")
      }
    }

    val columnNames = state.columns().filterNot(entry => entry._1 == Analyzers.COUNT_COL)

    val frequencies = state.frequencies().toSeq.map(row => Row.fromSeq(row._1 :+ row._2))
    val rowsRdd: RDD[Row] = session.sparkContext.parallelize(frequencies)

    val schema = columnNames.foldLeft(new StructType()) { (schema, col) =>
      println(col)
      schema.add(StructField(col._1, toSparkType(col._2)))
    }.add(StructField(Analyzers.COUNT_COL, LongType))

    session.sqlContext.createDataFrame(rowsRdd, schema)
      .coalesce(numPartitionsForHistogram)
      .write.parquet(s"$locationPrefix-$identifier-frequencies.pqt")

    writeToFileOnDfs(session, s"$locationPrefix-$identifier-num_rows.bin", allowOverwrite) {
      _.writeLong(state.numRows())
    }
  }

  private[this] def persistCorrelationState(state: CorrelationState, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.xAvg)
      out.writeDouble(state.yAvg)
      out.writeDouble(state.ck)
      out.writeDouble(state.xMk)
      out.writeDouble(state.yMk)
    }
  }

  private[this] def persistStandardDeviationState(
                                                   state: StandardDeviationState,
                                                   identifier: String) {

    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.avg)
      out.writeDouble(state.m2)
    }
  }

  private[this] def loadLongState(identifier: String): Long = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { _.readLong() }
  }

  private[this] def loadDoubleState(identifier: String): Double = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { _.readDouble() }
  }

  private[this] def loadLongLongState(identifier: String): NumMatchesAndCount = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      NumMatchesAndCount(in.readLong(), in.readLong())
    }
  }

  private[this] def loadDoubleLongState(identifier: String): MeanState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      MeanState(in.readDouble(), in.readLong())
    }
  }

  private[this] def loadBytes(identifier: String): Array[Byte] = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      val length = in.readInt()
      Array.fill(length) { in.readByte() }
    }
  }

  private[this] def loadDataframeLongState(identifier: String): JdbcFrequenciesAndNumRows = {

    val frequenciesDf = session.read.parquet(s"$locationPrefix-$identifier-frequencies.pqt")
    val numRows = readFromFileOnDfs(session, s"$locationPrefix-$identifier-num_rows.bin") {
      _.readLong()
    }

    var numColumns = 2

    var frequencies = Map[Seq[String], Long]()
    frequenciesDf.collect().foreach(row => {
      val rowSeq = row.toSeq
      val absolute = rowSeq.last.asInstanceOf[Long]
      val groupingColumns = rowSeq.dropRight(1).map(value => value match {
        case null => null
        case notNull => notNull.toString
      })
      frequencies += (groupingColumns -> absolute)
    })

    def toSqlDataType(sparkDataType: org.apache.spark.sql.types.DataType): String = {

      sparkDataType match {
        case BooleanType => "BOOLEAN"
        case StringType => "TEXT"
        case IntegerType => "INTEGER"
        case LongType => "BIGINT"
        case FloatType => "FLOAT"
        case DoubleType => "DOUBLE PRECISION"
        case DateType => "DATE"
        case TimestampType => "TIMESTAMP"

        case _ => throw new Exception(
          s"Could not find a conversion from Spark DataType to SQL DataType for $sparkDataType")
      }
    }

    val schema = frequenciesDf.schema
    val cols = schema.fields.foldLeft(mutable.LinkedHashMap[String, String]()) { (cols, field) =>
      cols += (field.name -> toSqlDataType(field.dataType))
    }

    JdbcFrequenciesAndNumRows.from(connection, cols, frequencies, numRows)
  }

  private[this] def loadCorrelationState(identifier: String): CorrelationState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      CorrelationState(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(),
        in.readDouble(), in.readDouble())
    }
  }

  private[this] def loadStandardDeviationState(identifier: String): StandardDeviationState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      StandardDeviationState(in.readDouble(), in.readDouble(), in.readDouble())
    }
  }
}

