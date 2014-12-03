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

// TODO: I reckon this can be implemented using a Stack (http://www.scala-lang.org/api/current/index.html#scala.collection.mutable.Stack)
object Field {
  private[util] val SEP = "/"

  private[util] def fromList(l: List[String]): Field = new Field(l.mkString(start = SEP, sep = SEP, end = ""))
  private[util] def fromReverseNameList(reverseNameList: List[String]): String =
    reverseNameList.reverse.mkString(start = SEP, sep = SEP, end = "")
  private[util] def toReverseNameList(s: String): List[String] = {
    { if (s.startsWith(SEP)) s.substring(1) else s }.split(SEP).reverse.toList
  }
  // creates a 'root'
  def fromRoot(): Field = fromRoot("root")
  def fromRoot(rootName: String): Field = new Field(SEP + rootName)
  // def apply(s: String): XPath = { if (s.startsWith(SEP)) (new XPath(s)) else (new XPath(SEP + s)) }
  def add(aPath: Field, aPart: String) = Field(s"${aPath.value}${SEP}${aPart}")
  def removeLast(aPath: Field) = {
    Field(fromReverseNameList(toReverseNameList(aPath.value).tail))
  }
}

