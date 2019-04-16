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

package com.amazon.deequ.runtime.jdbc.operators

import java.sql.Types

trait JdbcDataType {

  protected val validTypes: Seq[String] = Seq.empty[String]
  override def toString(): String = validTypes.head
}

trait JdbcNumericDataType extends JdbcDataType


object JdbcDataType {

  private val availableDataTypes = Seq(StringType, IntegerType, DoubleType, DecimalType,
    FloatType, TimestampType, BooleanType, LongType, ShortType, ByteType)

  def fromTypeName(dataTypeName: String): JdbcDataType = {
    availableDataTypes.find(dataType => dataType.validTypes.contains(dataTypeName)) match {
      case Some(dataType) => dataType
      case None =>
        throw new IllegalArgumentException(s"$dataTypeName is not in the list of supported data types.")
    }
  }

  def fromSqlType(dataType: Int): JdbcDataType = {

    dataType match {
      case Types.VARCHAR | Types.NULL => StringType
      case Types.INTEGER => IntegerType
      case Types.DOUBLE => DoubleType
      case Types.DECIMAL => DecimalType
      case Types.FLOAT | Types.REAL => FloatType
      case Types.TIMESTAMP => TimestampType
      case Types.BOOLEAN => BooleanType
      case Types.BIGINT => LongType
      case Types.SMALLINT => ShortType
      case Types.BINARY => ByteType

      case _ => throw new IllegalArgumentException(s"cannot get a datatype for $dataType")
    }
  }
}

object StringType extends JdbcDataType {
  override protected val validTypes = Seq("TEXT")
}

object IntegerType extends JdbcNumericDataType {
  override protected val validTypes = Seq("INT")
}

object DoubleType extends JdbcNumericDataType {
  override protected val validTypes = Seq("DOUBLE PRECISION", "DOUBLE")
}

object DecimalType extends JdbcNumericDataType {
  override protected val validTypes = Seq(s"DECIMAL")
}

case class NumericType(precision: Int, scale: Int) extends JdbcNumericDataType {
  override protected val validTypes = Seq(s"NUMERIC($precision, $scale)")
}

object FloatType extends JdbcNumericDataType {
  override protected val validTypes = Seq("FLOAT", "REAL")
}

object TimestampType extends JdbcDataType {
  override protected val validTypes = Seq("TIMESTAMP")
}

object BooleanType extends JdbcDataType {
  override protected val validTypes = Seq("BOOLEAN")
}

object LongType extends JdbcNumericDataType {
  override protected val validTypes = Seq("BIGINT")
}

object ShortType extends JdbcNumericDataType {
  override protected val validTypes = Seq("SMALLINT")
}

// TODO: looks like ByteType is a numeric type in Spark
object ByteType extends JdbcNumericDataType {
  override protected val validTypes = Seq("BYTEA")
}


case class JdbcStructType() {

  private var _fields: Seq[JdbcStructField] = Seq.empty
  private val a = 0
  def fields: Seq[JdbcStructField] = _fields

  override def toString: String = {
    fields.map(_.toString).mkString("(", ", ", ")")
  }

  def toStringWithoutDataTypes: String = {
    fields.map(_.name).mkString("(", ", ", ")")
  }

  def columnNamesEncoded(): String = {
    fields.map(_.name).mkString("(", ", ", ")")
  }

  def columnsNamesAsSet(): Set[String] = {
    fields.map(_.name).toSet
  }

  def columnsNamesAsSeq(): Seq[String] = {
    fields.map(_.name)
  }

  def add(field: JdbcStructField): JdbcStructType = {
    _fields = _fields :+ field
    this
  }

  def add(fields: Seq[JdbcStructField]): JdbcStructType = {
    _fields = _fields ++ fields
    this
  }
}

object JdbcStructType {

  def apply(fields: Seq[JdbcStructField]): JdbcStructType = {
    JdbcStructType().add(fields)
  }
}

case class JdbcStructField(name: String, dataType: JdbcDataType, nullable: Boolean = true) {
  override def toString: String = {
    s"$name $dataType"
  }
}