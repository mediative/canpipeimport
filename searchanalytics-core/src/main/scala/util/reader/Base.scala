package util.reader

import util.Logging
import util.wrapper.Wrapper
import scala.util.control.Exception._

/**
 * Generic 'type' reader.
 */
object Base {

  trait ReadSym[T] extends Logging {
    def read: String => Either[String, T]
    def readOrElse: String => T => T = {
      s =>
        defaultValue =>
          read(s) match {
            case Left(_) => defaultValue
            case Right(v) => v
          }

    }
  }

  // TODO: ensure that T is Numeric
  trait NumericReader[T] extends ReadSym[T] {
    def string2T: String => T
    def read: String => Either[String, T] =
      s =>
        s match {
          case s if s.isEmpty => Left(s"Field not present")
          case s => (catching(classOf[Exception]).either { string2T(s) }).left.map { t => s"'Parsing ${s}': ${t.toString}" }
        }

  }
  object LongReader extends NumericReader[Long] {
    def string2T: String => Long = _.toLong
  }
  object IntReader extends NumericReader[Int] {
    def string2T: String => Int = _.toInt
  }
  object DoubleReader extends NumericReader[Double] {
    def string2T: String => Double = _.toDouble
  }
  object BooleanReader extends ReadSym[Boolean] {
    def read: String => Either[String, Boolean] = {
      s =>
        s match {
          case s if s.isEmpty => Left(s"Field not present")
          case s =>
            s.trim.toUpperCase match {
              case "TRUE" | "1" | "T" | "TRUE" | "Y" | "YES" | "SUCCESS" => Right(true)
              case "FALSE" | "0" | "F" | "FALSE" | "N" | "NO" | "FAIL" | "FAILED" => Right(false)
              case x if (x.isEmpty) => Left(s"Field not present")
              case _ => Left(s"'${s}' can not be parsed as a Boolean")
            }
        }
    }
  }

}
