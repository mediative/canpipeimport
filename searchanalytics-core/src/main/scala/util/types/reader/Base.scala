package util.types.reader

import util.Logging
import scala.util.control.Exception._

/**
 * Generic 'type' reader.
 */
object Base {

  trait ReadSym[T] extends Logging {
    def read: String => Option[T]
    def default: T
    def readOrElse: String => T => T = {
      s =>
        defaultValue =>
          read(s) match {
            case None => defaultValue
            case Some(v) => v
          }

    }
    def apply(s: String): Option[T] = read(s)
    def apply(s: String, defaultValue: T): T = readOrElse(s)(defaultValue)
  }

  implicit object LongReader extends ReadSym[Long] {
    def read: String => Option[Long] = s => (catching(classOf[Exception]).opt { s.toLong })
    val default = Long.MinValue
  }
  implicit object IntReader extends ReadSym[Int] {
    def read: String => Option[Int] = s => (catching(classOf[Exception]).opt { s.toInt })
    val default = Int.MinValue
  }
  implicit object DoubleReader extends ReadSym[Double] {
    def read: String => Option[Double] = s => (catching(classOf[Exception]).opt { s.toDouble })
    val default = Double.MinPositiveValue
  }
  implicit object BooleanReader extends ReadSym[Boolean] {
    def read: String => Option[Boolean] = {
      s =>
        s match {
          case s if s.isEmpty => None
          case s =>
            s.trim.toUpperCase match {
              case "TRUE" | "1" | "T" | "TRUE" | "Y" | "YES" | "SUCCESS" => Some(true)
              case "FALSE" | "0" | "F" | "FALSE" | "N" | "NO" | "FAIL" | "FAILED" => Some(false)
              case x if (x.isEmpty) => None
              case _ => None
            }
        }
    }
    val default = false
  }

}
