package com.choreograph.tyda.bigquery

import java.math.MathContext
import java.nio.file.Files
import java.nio.file.Path

import scala.sys.process.Process
import scala.sys.process.ProcessLogger

import com.github.plokhotnyuk.jsoniter_scala.core.JsonReader
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import org.scalatest.Assertions.assume
import org.scalatest.Assertions.pending

import com.choreograph.tyda.Codec
import com.choreograph.tyda.Dataset
import com.choreograph.tyda.Decimal
import com.choreograph.tyda.Field
import com.choreograph.tyda.NumericLimits
import com.choreograph.tyda.Runner
import com.choreograph.tyda.json.CodecToJsoniter
import com.choreograph.tyda.rewrite.CollectionOrNullableCollectionCodec
import com.choreograph.tyda.shapeless3extras.mapConst
import com.choreograph.tyda.sql.DatasetToSqlError
import com.choreograph.tyda.sql.RenderedMultiStatement
import com.choreograph.tyda.sql.SqlDialect
import com.choreograph.tyda.sql.toSql
import com.choreograph.tyda.testsuites.BigQueryIntegrationTestEnvVariables
import com.choreograph.tyda.unreachable

object GoogleSqlTestRunner {
  private val ExecutableEnv = "TYDA_GOOGLESQL_EXECUTE_QUERY"

  def configuredOrSkip: GoogleSqlTestRunner = {
    BigQueryIntegrationTestEnvVariables.skipIfProjectIsSet()
    sys.env.get(ExecutableEnv).map(Path.of(_)) match {
      case Some(executable) if Files.isExecutable(executable) => new GoogleSqlTestRunner(executable)
      case _ =>
        assume(
          condition = false,
          s"Set $ExecutableEnv to the executable provided by dev/download-googlesql.sh to run local GoogleSQL tests"
        )
        unreachable("Test skipped by assume")
    }
  }

  private[bigquery] def errorMessageFromBoxOutput(output: String, query: String, stderr: String): String =
    output
      .linesIterator
      .collectFirst { case line if line.startsWith("ERROR: ") => line.stripPrefix("ERROR: ") }
      .getOrElse(
        s"GoogleSQL returned no JSON output and no recognized error. SQL:\n$query\nStandard output:\n$output\nStandard error:\n$stderr"
      )

  private[bigquery] def escapeMacrosInStringLiterals(sql: String): String = {
    val result = StringBuilder()
    var inString = false
    var index = 0
    while index < sql.length do
      sql(index) match {
        case '\'' =>
          inString = !inString
          result.append('\'')
          index += 1
        case '\\' if inString && index + 1 < sql.length =>
          result.append(sql(index)).append(sql(index + 1)): Unit
          index += 2
        case '$' if inString =>
          result.append("$' '")
          index += 1
        case character =>
          result.append(character)
          index += 1
      }
    result.result()
  }
}

final class GoogleSqlTestRunner private (executable: Path) extends Runner {
  def sql(ds: Dataset[?] | Dataset.Action): RenderedMultiStatement =
    toSql(ds, SqlDialect.GoogleSql) match {
      case Left(DatasetToSqlError.RequiresUdfCapability(_)) =>
        pending
        unreachable("Test should be skipped by pending")
      case Left(DatasetToSqlError.NotImplemented(msg)) =>
        pending
        unreachable(s"Unimplemented feature: $msg")
      case Right(plan) => plan
    }

  def collect[T](ds: Dataset[T]): Seq[T] = {
    val rendered = sql(ds).single
    val query = GoogleSqlTestRunner.escapeMacrosInStringLiterals(
      s"SELECT TO_JSON_STRING(result) AS value FROM ($rendered) AS result"
    )
    val encodedRows = run(query)
    val jsonCodec = GoogleSqlJsonDecoder.create(using ds.codec)
    encodedRows.map(encoded => readFromString(encoded)(using jsonCodec))
  }

  def execute(ds: Dataset.Action): Unit =
    throw UnsupportedOperationException("GoogleSQL reference execution does not support BigQuery actions")

  def explain[T](ds: Dataset[T]): String = sql(ds).single
  def explain(action: Dataset.Action): String = sql(action).single

  private def run(query: String): Seq[String] = {
    val queryFile = Files.createTempFile("tyda-googlesql-", ".sql")
    try {
      Files.writeString(queryFile, query)
      val (exitCode, stdout, stderr) = runQuery(queryFile, "json")
      if exitCode != 0 then
        throw RuntimeException(
          s"GoogleSQL exited with status $exitCode. SQL:\n$query\nStandard output:\n$stdout\nStandard error:\n$stderr"
        )
      val output = stdout.trim
      if output.isEmpty then {
        val (boxExitCode, boxOutput, boxError) = runQuery(queryFile, "box")
        if boxExitCode != 0 then
          throw RuntimeException(
            s"GoogleSQL exited with status $boxExitCode. SQL:\n$query\nStandard output:\n$boxOutput\nStandard error:\n$boxError"
          )
        throw RuntimeException(GoogleSqlTestRunner.errorMessageFromBoxOutput(boxOutput, query, boxError))
      } else
        try readFromString(output)(using GoogleSqlResultCodec)
        catch {
          case error: Throwable => throw RuntimeException(
              s"Unable to decode GoogleSQL result. SQL:\n$query\nOutput:\n$output",
              error
            )
        }
    } finally Files.deleteIfExists(queryFile): Unit
  }

  private def runQuery(queryFile: Path, outputMode: String): (Int, String, String) = {
    val command =
      Seq(executable.toString, "--catalog=none", s"--output_mode=$outputMode", s"--sql_file=$queryFile")
    val stdout = StringBuilder()
    val stderr = StringBuilder()
    val logger = ProcessLogger(
      line => { stdout.append(line).append('\n'): Unit },
      line => { stderr.append(line).append('\n'): Unit }
    )
    val exitCode = Process(command).!(logger)
    (exitCode, stdout.result(), stderr.result())
  }
}

private[bigquery] object GoogleSqlJsonDecoder {
  def create[T: Codec]: JsonValueCodec[T] =
    Codec[T] match {
      case Codec.Product(_, _, _) => fromReaderWriter[T]
      case Codec.FromInjection(injection, inner) =>
        xmap(create(using inner), injection.apply, injection.invert)
      case _ => xmap(create[(value: T)], (value = _), _.value)
    }

  private def fromReaderWriter[T: Codec]: JsonValueCodec[T] = {
    val jsonCodec = CodecToJsoniter.create[T]
    val read = reader(summon[Codec[T]])
    new JsonValueCodec[T] {
      def decodeValue(in: JsonReader, default: T): T = read(in)

      def encodeValue(value: T, out: JsonWriter): Unit = jsonCodec.encodeValue(value, out)

      def nullValue: T = jsonCodec.nullValue
    }
  }

  private def xmap[T: Codec, A](inner: JsonValueCodec[A], to: T => A, from: A => T): JsonValueCodec[T] =
    new JsonValueCodec[T] {
      def decodeValue(in: JsonReader, default: T): T = from(inner.decodeValue(in, inner.nullValue))

      def encodeValue(value: T, out: JsonWriter): Unit = inner.encodeValue(to(value), out)

      def nullValue: T = CodecToJsoniter.create[T].nullValue
    }

  private type Reader[T] = JsonReader => T

  private def reader[T](codec: Codec[T]): Reader[T] =
    codec match {
      case Codec.Boolean => standardReader(Codec.Boolean)
      case Codec.Byte => checkedIntegralReader[Byte]
      case Codec.Short => checkedIntegralReader[Short]
      case Codec.Int => checkedIntegralReader[Int]
      case Codec.Long => standardReader(Codec.Long)
      case Codec.Float => standardReader(Codec.Float)
      case Codec.Double => standardReader(Codec.Double)
      case Codec.String => standardReader(Codec.String)
      case Codec.Bytes => standardReader(Codec.Bytes)
      case decimal @ Codec.Decimal(precision, scale) => in => {
          val value =
            if in.nextValueIsString() then BigDecimal(in.readString(null))
            else in.readBigDecimal(null, MathContext.UNLIMITED, 1000, 1000)
          Decimal(using decimal.valid)(value).getOrElse(
            throw RuntimeException(s"Value $value cannot be represented as Decimal($precision, $scale)")
          )
        }
      case Codec.Date => standardReader(Codec.Date)
      case Codec.TimestampMicros => standardReader(Codec.TimestampMicros)
      case Codec.DurationMicros => standardReader(Codec.DurationMicros)
      case Codec.Option(element @ Codec.Option(_)) => nestedOptionReader(element)
      case Codec.Option(element) =>
        val elementReader = reader(element)
        in =>
          if in.isNextToken('n') then in.readNullOrError(None, "expected null or value")
          else {
            in.rollbackToken()
            Some(elementReader(in))
          }
      case Codec.Seq(element) =>
        val elementReader = reader(element)
        val wrapped = CollectionOrNullableCollectionCodec.unapply(element).isDefined
        in => seqReader(in, if wrapped then wrappedReader(elementReader) else elementReader)
      case Codec.Map(given Codec[k], given Codec[v]) =>
        val pairReader = reader[Seq[(key: k, value: v)]](summon)
        in => pairReader(in).map { case (key, value) => key -> value }.toMap
      case product @ Codec.Product(_, _, _) => productReader(product)
      case Codec.FromInjection(injection, inner) => reader(inner).andThen(injection.invert)
    }

  private def standardReader[T](codec: Codec[T]): Reader[T] = {
    given Codec[T] = codec
    val jsonCodec = CodecToJsoniter.unwrapped[T]
    in => jsonCodec.decodeValue(in, jsonCodec.nullValue)
  }

  private def checkedIntegralReader[A: {NumericLimits as limits, Numeric as numeric}]: Reader[A] =
    in => {
      import numeric.mkNumericOps
      val value = if in.nextValueIsString() then in.readStringAsLong() else in.readLong()
      if value < limits.min.toLong || value > limits.max.toLong then
        throw RuntimeException(s"Value $value is out of range for target type")
      numeric.fromInt(value.toInt)
    }

  private def nestedOptionReader[T](element: Codec.Option[T]): Reader[Option[Option[T]]] = {
    given Codec[T] = element.element
    val structReader = reader[Option[(value: Option[T])]](summon)
    in => structReader(in).map(_.value)
  }

  private def seqReader[T](in: JsonReader, elementReader: Reader[T]): Seq[T] =
    if !in.isNextToken('[') then in.decodeError("Expected GoogleSQL array")
    else if in.isNextToken(']') then Seq.empty
    else {
      in.rollbackToken()
      val values = Vector.newBuilder[T]
      while {
        values += elementReader(in)
        in.isNextToken(',')
      } do ()
      if !in.isCurrentToken(']') then in.arrayEndOrCommaError()
      values.result()
    }

  private def wrappedReader[T](read: Reader[T]): Reader[T] =
    in => {
      if !in.isNextToken('{') then in.decodeError("Expected GoogleSQL array element wrapper")
      var value: Option[T] = None
      if !in.isNextToken('}') then {
        in.rollbackToken()
        while {
          in.readKeyAsString() match {
            case "value" => value = Some(read(in))
            case _ => in.skip()
          }
          in.isNextToken(',')
        } do ()
        if !in.isCurrentToken('}') then in.objectEndOrCommaError()
      }
      value.getOrElse(in.decodeError("GoogleSQL array element wrapper has no value field"))
    }

  private def productReader[T](codec: Codec.Product[T]): Reader[T] = {
    val fields = codec.fields.mapConst[Field[?]]([t] => identity(_)).toArray
    val readers = fields.map(_.codec).map(reader)
    val required = fields.map(!_.codec.isInstanceOf[Codec.Option[?]])
    val nameToIndex = fields.map(_.name).zipWithIndex.toMap
    in =>
      def fromValues(values: Array[Any]): T = {
        required.foldLeft(0) { (index, isRequired) =>
          if values(index) == null then
            if isRequired then in.decodeError("missing required field " + fields(index).name)
            else values(index) = None
          index + 1
        }: Unit
        codec.fromProduct(Tuple.fromArray(values))
      }
      if !in.isNextToken('{') then in.decodeError("Expected GoogleSQL struct")
      else if in.isNextToken('}') then fromValues(new Array[Any](fields.size))
      else {
        in.rollbackToken()
        val values = new Array[Any](fields.size)
        while {
          val fieldName = in.readKeyAsString()
          nameToIndex.get(fieldName) match {
            case Some(index) =>
              if values(index) != null then in.decodeError("duplicate key " + fieldName)
              values(index) = readers(index)(in)
            case _ => in.skip()
          }
          in.isNextToken(',')
        } do ()
        if !in.isCurrentToken('}') then in.objectEndOrCommaError()
        fromValues(values)
      }
  }

  extension (in: JsonReader)
    private def nextValueIsString(): Boolean = {
      val result = in.isNextToken('"')
      in.rollbackToken()
      result
    }
}

private[bigquery] object GoogleSqlResultCodec extends JsonValueCodec[Seq[String]] {
  def decodeValue(in: JsonReader, default: Seq[String]): Seq[String] = {
    if !in.isNextToken('{') then in.decodeError("Expected GoogleSQL JSON result object")
    val rows = Vector.newBuilder[String]
    if !in.isNextToken('}') then {
      in.rollbackToken()
      while {
        in.readKeyAsString() match {
          case "row" => readRows(in, rows)
          case _ => in.skip()
        }
        in.isNextToken(',')
      } do ()
      if !in.isCurrentToken('}') then in.objectEndOrCommaError()
    }
    rows.result()
  }

  def encodeValue(x: Seq[String], out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter): Unit =
    out.writeArrayStart()
    x.foreach(out.writeVal)
    out.writeArrayEnd()

  def nullValue: Seq[String] = Seq.empty

  private def readRows(
      in: JsonReader,
      rows: scala.collection.mutable.Builder[String, Vector[String]]
  ): Unit = {
    if !in.isNextToken('[') then in.decodeError("Expected GoogleSQL result rows")
    else if !in.isNextToken(']') then {
      in.rollbackToken()
      while {
        rows += readRow(in)
        in.isNextToken(',')
      } do ()
      if !in.isCurrentToken(']') then in.arrayEndOrCommaError()
    }
  }

  private def readRow(in: JsonReader): String = {
    if !in.isNextToken('{') then in.decodeError("Expected GoogleSQL result row")
    var value: Option[String] = None
    if !in.isNextToken('}') then {
      in.rollbackToken()
      while {
        in.readKeyAsString() match {
          case "value" => value = Some(in.readString(null))
          case _ => in.skip()
        }
        in.isNextToken(',')
      } do ()
      if !in.isCurrentToken('}') then in.objectEndOrCommaError()
    }
    value.getOrElse(in.decodeError("GoogleSQL result row has no value column"))
  }
}
