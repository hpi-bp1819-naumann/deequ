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

package com.amazon.deequ.runtime.jdbc

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

import com.amazon.deequ.runtime.jdbc.operators._
import com.amazon.deequ.runtime.{StateLoader, StatePersister}

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

private object StateInformation {
  // 4 byte for the id of an DataTypeInstances entry + 8 byte for the (Long) count of that data type
  // as used in HdfsStateProvider.persistDataTypeState and HdfsStateProvider.loadDataTypeState
  final val dataTypeCountEntrySizeInBytes = 4 + 8
}

/** Load a stored state for an operator */
trait JdbcStateLoader extends StateLoader[Connection] {
  def load[S <: State[_]](operator: Operator[S, _],
                          connection: Option[Connection] = None): Option[S]
}

/** Persist a state for an operator */
trait JdbcStatePersister extends StatePersister[Connection] {
  def persist[S <: State[_]](operator: Operator[S, _], state: S)
}

/** Store states in memory */
case class InMemoryJdbcStateProvider() extends JdbcStateLoader with JdbcStatePersister {

  private[this] val statesByAnalyzer = new ConcurrentHashMap[Operator[_, _], State[_]]()

  override def load[S <: State[_]](operator: Operator[S, _],
                                   connection: Option[Connection] = None): Option[S] = {
    Option(statesByAnalyzer.get(operator).asInstanceOf[S])
  }

  override def persist[S <: State[_]](operator: Operator[S, _], state: S): Unit = {
    statesByAnalyzer.put(operator, state)
  }

  override def toString: String = {
    val buffer = new StringBuilder()
    statesByAnalyzer.foreach { case (operator, state) =>
      buffer.append(operator.toString)
      buffer.append(" => ")
      buffer.append(state.toString)
      buffer.append("\n")
    }

    buffer.toString
  }
}

/** Store states on a filesystem (supports local disk) */
case class FileSystemJdbcStateProvider(
    locationPrefix: String,
    numPartitionsForHistogram: Int = 10,
    allowOverwrite: Boolean = false)
  extends JdbcStateLoader with JdbcStatePersister {

  private[this] def toIdentifier[S <: State[_]](operator: Operator[S, _]): String = {
    MurmurHash3.stringHash(operator.toString, 42).toString
  }

  override def persist[S <: State[_]](operator: Operator[S, _], state: S): Unit = {

    val identifier = toIdentifier(operator)

    operator match {
      case _: SizeOp =>
        persistLongState(state.asInstanceOf[NumMatches].numMatches, identifier)

      case _ : CompletenessOp | _ : ComplianceOp | _ : PatternMatchOp =>
        persistLongLongState(state.asInstanceOf[NumMatchesAndCount], identifier)

      case _: SumOp =>
        persistDoubleState(state.asInstanceOf[SumState].sum, identifier)

      case _: MeanOp =>
        persistDoubleLongState(state.asInstanceOf[MeanState], identifier)

      case _: MinimumOp =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _: MaximumOp =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _: FrequencyBasedOperator | _: HistogramOp =>
        persistFrequenciesLongState(state.asInstanceOf[FrequenciesAndNumRows], identifier)

      case _: DataTypeOp =>
        val histogram = state.asInstanceOf[DataTypeHistogram]
        persistBytes(DataTypeHistogram.toBytes(histogram.numNull, histogram.numFractional,
          histogram.numIntegral, histogram.numBoolean, histogram.numString), identifier)

      case _ : CorrelationOp =>
        persistCorrelationState(state.asInstanceOf[CorrelationState], identifier)

      case _ : StandardDeviationOp =>
        persistStandardDeviationState(state.asInstanceOf[StandardDeviationState], identifier)

      case _ : UniquenessOp =>
        persistFrequenciesLongState(state.asInstanceOf[FrequenciesAndNumRows], identifier)

      // missing within spark StateProvider?
      case _: EntropyOp =>
        persistFrequenciesLongState(state.asInstanceOf[FrequenciesAndNumRows], identifier)

      case _ =>
        throw new IllegalArgumentException(s"Unable to persist state for operator $operator.")
    }
  }

  override def load[S <: State[_]](operator: Operator[S, _],
                                   connection: Option[Connection] = None): Option[S] = {

    val identifier = toIdentifier(operator)

    val state: Any = operator match {

      case _ : SizeOp => NumMatches(loadLongState(identifier))

      case _ : CompletenessOp | _ : ComplianceOp | _ : PatternMatchOp => loadLongLongState(identifier)

      case _ : SumOp => SumState(loadDoubleState(identifier))

      case _ : MeanOp => loadDoubleLongState(identifier)

      case _ : MinimumOp => MinState(loadDoubleState(identifier))

      case _ : MaximumOp => MaxState(loadDoubleState(identifier))

      case _ : FrequencyBasedOperator | _ : HistogramOp => loadFrequenciesLongState(identifier, connection)

      case _ : DataTypeOp => DataTypeHistogram.fromBytes(loadBytes(identifier))

      /* case _ : ApproxCountDistinctOp =>
        DeequHyperLogLogPlusPlusUtils.wordsFromBytes(loadBytes(identifier)) */

      case _ : CorrelationOp => loadCorrelationState(identifier)

      case _ : StandardDeviationOp => loadStandardDeviationState(identifier)

      /*case _: ApproxQuantileOp =>
        val percentileDigest = ApproximatePercentile.serializer.deserialize(loadBytes(identifier))
        ApproxQuantileState(percentileDigest)*/

      case _ =>
        throw new IllegalArgumentException(s"Unable to load state for operator $operator.")
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
      state: FrequenciesAndNumRows,
      identifier: String)
    : Unit = {

    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>

      val (cols, data) = state.frequencies()

      out.writeInt(cols.fields.size)

      for (field <- cols.fields) {
        out.writeInt(field.name.length)
        for (char <- field.name) {
          out.writeChar(char)
        }

        out.writeInt(field.dataType.toString().length)
        for (char <- field.dataType.toString()) {
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

  private[this] def loadFrequenciesLongState(identifier: String, connection: Option[Connection]):
  FrequenciesAndNumRows = {

    if (connection.isEmpty) {
      throw new Exception("A Jdbc Connection is required to load State from Disk.")
    }

    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>

      val numberOfCols = in.readInt()

      val schema = JdbcStructType()

      for (_ <- 1 to numberOfCols) {
        val colNameLength = in.readInt()
        val colName: String = (for (_ <- 1 to colNameLength) yield in.readChar()).mkString

        val colDataTypeLength = in.readInt()
        val colDataType: String = (for (_ <- 1 to colDataTypeLength) yield in.readChar()).mkString

        schema.add(JdbcStructField(colName, JdbcDataType.fromTypeName(colDataType)))
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
      FrequenciesAndNumRows.from(connection.get, schema, frequencies, numRows)
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
