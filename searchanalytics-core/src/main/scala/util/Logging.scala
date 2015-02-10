package util

/*
 * TODO: actually insert library here!!!!
 * eg, import com.typesafe.scalalogging.slf4j.StrictLogging
 */
/**
 * Wrap-up against any logger we'd like.
 */

object Logging extends Serializable {
  object Level extends Enumeration {
    type Level = Value
    val Silent, Info, Debug, Error = Value
  }
}

trait Logging extends Serializable {
  import Logging.Level._

  def level: Level = Logging.Level.Info

  object logger extends Serializable {
    def info(msg: String) = if (level == Info) println(s"[INFO] ${msg}")
    def debug(msg: String) = if ((level == Info) || (level == Debug)) println(s"[DEBUG] ${msg}")
    def error(msg: String) = println(s"[ERROR] ${msg}")
  }
}

trait InfoLogging extends Logging {
  override val level = Logging.Level.Info
}

trait SilentLogging extends Logging {
  override val level = Logging.Level.Silent
}

trait DebugLogging extends Logging {
  override val level = Logging.Level.Debug
}

trait ErrorLogging extends Logging {
  override val level = Logging.Level.Error
}
