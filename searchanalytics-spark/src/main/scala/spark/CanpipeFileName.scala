package spark

import _root_.util.wrapper.{ String => StringWrapper }

// TODO: can I see somehow set up the inheritance with spark.util.wrapper.HDFSFileName
case class CanpipeFileName(value: String) extends StringWrapper with Serializable
