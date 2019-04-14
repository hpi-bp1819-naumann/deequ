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
  override protected val validTypes = Seq("DOUBLE", "DOUBLE PRECISION")
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


case class JdbcStructType(fields: Seq[JdbcStructField]) {
  override def toString: String = {
    fields.map(_.toString).mkString("(", ", ", ")")
  }

  def columnNames(): String = {
    fields.map(_.name).mkString("(", ", ", ")")
  }
}

case class JdbcStructField(name: String, dataType: JdbcDataType, nullable: Boolean = true) {
  override def toString: String = {
    s"$name $dataType"
  }
}