package com.choreograph.tyda.bigquery

import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import org.scalatest.funsuite.AnyFunSuite

import com.choreograph.tyda.Binary
import com.choreograph.tyda.Codec
import com.choreograph.tyda.Dataset
import com.choreograph.tyda.Decimal
import com.choreograph.tyda.functions.fromBase64

class GoogleSqlTestRunnerSpec extends AnyFunSuite {
  test("decode GoogleSQL JSON result rows") {
    val output = """{"row":[{"value":"{\"value\":1}"},{"value":"{\"value\":2}"}]}"""
    assert(readFromString(output)(using GoogleSqlResultCodec) == Seq("{\"value\":1}", "{\"value\":2}"))
  }

  test("decode GoogleSQL JSON result with no rows") {
    assert(readFromString("{} ")(using GoogleSqlResultCodec) == Seq.empty)
  }

  test("extract execution errors from GoogleSQL box output") {
    assert(
      GoogleSqlTestRunner.errorMessageFromBoxOutput("ERROR: OUT_OF_RANGE: value is out of range\n", "", "") ==
        "OUT_OF_RANGE: value is out of range"
    )
  }

  test("report unexpected GoogleSQL box output") {
    assert(
      GoogleSqlTestRunner.errorMessageFromBoxOutput("", "SELECT 1", "unexpected output") ==
        "GoogleSQL returned no JSON output and no recognized error. SQL:\nSELECT 1\nStandard output:\n\nStandard error:\nunexpected output"
    )
  }

  test("escape GoogleSQL macros inside string literals") {
    assert(
      GoogleSqlTestRunner.escapeMacrosInStringLiterals("SELECT '$A(', '\\$B(', $parameter") ==
        "SELECT '$' 'A(', '\\$B(', $parameter"
    )
  }

  test("match BigQuery Base64 whitespace handling") {
    val values = Seq("++ \t\r\n", "++\u001C", "++\u1680")
    val result = GoogleSqlTestRunner.configuredOrSkip.collect(Dataset.from(values).select(fromBase64))
    assert(result == Seq(Some(Binary.fromArray(Array(-5))), None, None))
  }

  test("decode nested arrays wrapped for GoogleSQL") {
    given Codec[Seq[Seq[Int]]] = summon
    val encoded = """{"value":[{"value":[1,2]},{"value":[]}]}"""
    assert(readFromString(encoded)(using GoogleSqlJsonDecoder.create) == Seq(Seq(1, 2), Seq.empty))
  }

  test("decode nullable arrays wrapped for GoogleSQL") {
    given Codec[Seq[Option[Seq[Int]]]] = summon
    val encoded = """{"value":[{"value":[1,2]},{"value":null},{"value":[]}]}"""
    assert(
      readFromString(encoded)(using GoogleSqlJsonDecoder.create) ==
        Seq(Some(Seq(1, 2)), None, Some(Seq.empty))
    )
  }

  test("reject integral values outside the target type's range") {
    val error = intercept[RuntimeException](readFromString("""{"value":2147483648}""")(using
      GoogleSqlJsonDecoder.create[Int]
    ))
    assert(error.getMessage.contains("out of range"))
  }

  test("reject decimal values outside the target type's range") {
    val error = intercept[RuntimeException](readFromString("""{"value":"100"}""")(using
      GoogleSqlJsonDecoder.create[Decimal[2, 0]]
    ))
    assert(error.getMessage.contains("cannot be represented as Decimal(2, 0)"))
  }
}
