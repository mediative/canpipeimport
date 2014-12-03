package util

/*
 * TODO: actually insert library here!!!!
 * eg, import com.typesafe.scalalogging.slf4j.StrictLogging
 */
/**
 * Wrap-up against any logger we'd like.
 */
trait Logging extends Serializable {
  object logger extends Serializable {
    def info(msg: String) = println(s"[INFO] ${msg}")
    def debug(msg: String) = println(s"[DEBUG] ${msg}")
    def error(msg: String) = println(s"[ERROR] ${msg}")
  }
}
