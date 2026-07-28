package com.choreograph.tyda.sql.spark.overflow

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

private def primitiveArguments(
    operationName: String,
    arguments: Array[ObjectInspector],
    expected: PrimitiveObjectInspector.PrimitiveCategory
): Array[PrimitiveObjectInspector] = {
  require(arguments.length == 2, s"$operationName requires exactly two arguments")
  arguments.map {
    case inspector: PrimitiveObjectInspector if inspector.getPrimitiveCategory == expected => inspector
    case _ => throw IllegalArgumentException(s"$operationName requires $expected arguments")
  }
}

private abstract class CheckedFloatOperation(
    operationName: String,
    operation: (Float, Float) => Float,
    checkDivisionByZero: Boolean = false
) extends GenericUDF {
  private var argumentInspectors = Array.empty[PrimitiveObjectInspector]

  final override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    argumentInspectors =
      primitiveArguments(operationName, arguments, PrimitiveObjectInspector.PrimitiveCategory.FLOAT)
    PrimitiveObjectInspectorFactory.javaFloatObjectInspector
  }

  final override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val lhsValue = arguments(0).get()
    val rhsValue = arguments(1).get()
    if lhsValue == null || rhsValue == null then null
    else {
      // TYPE SAFETY: initialize verifies FLOAT arguments, whose Java representation is boxed Float.
      val lhs = argumentInspectors(0)
        .getPrimitiveJavaObject(lhsValue)
        .asInstanceOf[java.lang.Float]
        .floatValue
      // TYPE SAFETY: initialize verifies FLOAT arguments, whose Java representation is boxed Float.
      val rhs = argumentInspectors(1)
        .getPrimitiveJavaObject(rhsValue)
        .asInstanceOf[java.lang.Float]
        .floatValue
      if checkDivisionByZero && rhs == 0.0f then throw ArithmeticException("division by zero")
      val result = operation(lhs, rhs)
      if !lhs.isInfinite && !rhs.isInfinite && !result.isNaN && !(Math.abs(result) <= Float.MaxValue) then
        throw RuntimeException("Float overflow")
      java.lang.Float.valueOf(result)
    }
  }

  final override def getDisplayString(children: Array[String]): String =
    s"$operationName(${children.mkString(", ")})"
}

private abstract class CheckedDoubleOperation(
    operationName: String,
    operation: (Double, Double) => Double,
    checkDivisionByZero: Boolean = false
) extends GenericUDF {
  private var argumentInspectors = Array.empty[PrimitiveObjectInspector]

  final override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    argumentInspectors =
      primitiveArguments(operationName, arguments, PrimitiveObjectInspector.PrimitiveCategory.DOUBLE)
    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
  }

  final override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val lhsValue = arguments(0).get()
    val rhsValue = arguments(1).get()
    if lhsValue == null || rhsValue == null then null
    else {
      // TYPE SAFETY: initialize verifies DOUBLE arguments, whose Java representation is boxed Double.
      val lhs = argumentInspectors(0)
        .getPrimitiveJavaObject(lhsValue)
        .asInstanceOf[java.lang.Double]
        .doubleValue
      // TYPE SAFETY: initialize verifies DOUBLE arguments, whose Java representation is boxed Double.
      val rhs = argumentInspectors(1)
        .getPrimitiveJavaObject(rhsValue)
        .asInstanceOf[java.lang.Double]
        .doubleValue
      if checkDivisionByZero && rhs == 0.0 then throw ArithmeticException("division by zero")
      val result = operation(lhs, rhs)
      if !lhs.isInfinite && !rhs.isInfinite && !result.isNaN && !(Math.abs(result) <= Double.MaxValue) then
        throw RuntimeException("Double overflow")
      java.lang.Double.valueOf(result)
    }
  }

  final override def getDisplayString(children: Array[String]): String =
    s"$operationName(${children.mkString(", ")})"
}

final class CheckedFloatAdd extends CheckedFloatOperation("tyda_float_add", _ + _)
final class CheckedFloatSubtract extends CheckedFloatOperation("tyda_float_subtract", _ - _)
final class CheckedFloatMultiply extends CheckedFloatOperation("tyda_float_multiply", _ * _)
final class CheckedFloatQuotient
    extends CheckedFloatOperation("tyda_float_divide", _ / _, checkDivisionByZero = true)

final class CheckedDoubleAdd extends CheckedDoubleOperation("tyda_double_add", _ + _)
final class CheckedDoubleSubtract extends CheckedDoubleOperation("tyda_double_subtract", _ - _)
final class CheckedDoubleMultiply extends CheckedDoubleOperation("tyda_double_multiply", _ * _)
final class CheckedDoubleQuotient
    extends CheckedDoubleOperation("tyda_double_divide", _ / _, checkDivisionByZero = true)
