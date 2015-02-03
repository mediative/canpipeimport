package spark.util.wrapper

import util.wrapper.{ String => StringWrapper }
case class HDFSFileName(name: String) extends StringWrapper {
  val value = name
}
