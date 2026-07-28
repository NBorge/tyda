package com.choreograph.tyda.sql

import java.util.UUID

import scala.collection.mutable

import com.choreograph.tyda.Codec

private[sql] enum FloatingOperation(val name: String, val symbol: String) {
  case Add extends FloatingOperation("add", "+")
  case Subtract extends FloatingOperation("subtract", "-")
  case Multiply extends FloatingOperation("multiply", "*")
  case Quotient extends FloatingOperation("divide", "/")
}

private[sql] enum FloatingType(val name: String, val maxValue: String, val errorMessage: String) {
  case Float extends FloatingType("float", "3.4028234663852886E38", "Float overflow")
  case Double extends FloatingType("double", "1.7976931348623157E308", "Double overflow")
}

private[sql] object FloatingType {
  def from(codec: Codec[?]): Option[FloatingType] =
    codec match {
      case Codec.Float => Some(FloatingType.Float)
      case Codec.Double => Some(FloatingType.Double)
      case _ => None
    }
}

private final class FloatingOverflowFunctionRegistry {
  private val used = mutable.LinkedHashSet.empty[(FloatingOperation, FloatingType)]
  private val sparkNamespace = UUID.randomUUID().toString.replace("-", "")

  def register(dialect: SqlDialect, operation: FloatingOperation, resultType: FloatingType): Option[String] =
    val isSupported = (dialect.floatingOverflowFunctions, resultType) match {
      case (SqlDialect.FloatingOverflowFunctions.BigQuery, FloatingType.Float) => true
      case (SqlDialect.FloatingOverflowFunctions.Spark, _) => true
      case _ => false
    }
    if isSupported then {
      used += ((operation, resultType))
      Some(functionName(dialect, operation, resultType))
    } else None

  def setup(dialect: SqlDialect): Seq[String] =
    used
      .toSeq
      .map { case (operation, resultType) =>
        dialect.floatingOverflowFunctions match {
          case SqlDialect.FloatingOverflowFunctions.BigQuery => bigQueryFunction(operation, resultType)
          case SqlDialect.FloatingOverflowFunctions.Spark => sparkFunction(operation, resultType)
          case SqlDialect.FloatingOverflowFunctions.Disabled =>
            throw IllegalStateException("A disabled floating overflow function registry was used")
        }
      }

  def teardown(dialect: SqlDialect): Seq[String] =
    dialect.floatingOverflowFunctions match {
      case SqlDialect.FloatingOverflowFunctions.Spark => used
          .toSeq
          .map { case (operation, resultType) =>
            s"DROP TEMPORARY FUNCTION IF EXISTS ${functionName(dialect, operation, resultType)}"
          }
      case _ => Seq.empty
    }

  private def functionName(
      dialect: SqlDialect,
      operation: FloatingOperation,
      resultType: FloatingType
  ): String =
    dialect.floatingOverflowFunctions match {
      case SqlDialect.FloatingOverflowFunctions.Spark =>
        s"tyda_${sparkNamespace}_${resultType.name}_${operation.name}"
      case _ => s"tyda_${resultType.name}_${operation.name}"
    }

  private def bigQueryFunction(operation: FloatingOperation, resultType: FloatingType): String = {
    val name = functionName(SqlDialect.BigQuery, operation, resultType)
    s"""CREATE TEMP FUNCTION $name(lhs FLOAT64, rhs FLOAT64)
       |RETURNS FLOAT64
       |AS (
       |  CASE
       |    WHEN NOT (abs(lhs) = CAST('Infinity' AS FLOAT64))
       |      AND NOT (abs(rhs) = CAST('Infinity' AS FLOAT64))
       |      AND NOT is_nan(lhs ${operation.symbol} rhs)
       |      AND NOT (abs(lhs ${operation.symbol} rhs) <= CAST(${resultType.maxValue} AS FLOAT64))
       |    THEN CAST(error('${resultType.errorMessage}') AS FLOAT64)
       |    ELSE lhs ${operation.symbol} rhs
       |  END
       |)""".stripMargin
  }

  private def sparkFunction(operation: FloatingOperation, resultType: FloatingType): String = {
    val name = functionName(SqlDialect.Spark, operation, resultType)
    val className = operation match {
      case FloatingOperation.Add => "Add"
      case FloatingOperation.Subtract => "Subtract"
      case FloatingOperation.Multiply => "Multiply"
      case FloatingOperation.Quotient => "Quotient"
    }
    s"CREATE TEMPORARY FUNCTION $name AS 'com.choreograph.tyda.sql.spark.overflow.Checked${resultType
        .name
        .capitalize}$className'"
  }
}
