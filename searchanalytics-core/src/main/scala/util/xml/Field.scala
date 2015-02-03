package util.xml

import util.wrapper.{ String => StringWrapper }

/**
 * TODO: fill this up
 * @param value
 */
case class Field(value: String) extends StringWrapper {
  import Field._

  def asString = value
  def asList: List[String] = { // TODO: this should be 'asIterator'
    (if (value.startsWith(SEP)) value.substring(1) else value).split(SEP).toList
  }
}

object Field {
  private[util] val SEP = "/"
}
