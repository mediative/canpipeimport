package canpipe.xml

import util.wrapper.Wrapper

trait Elem extends Wrapper[xml.Elem] {
  def value: xml.Elem
}

object Elem {

  import util.xml.Base._
  def apply(n: scala.xml.Elem): Option[Elem] = {
    val totalElems = n.child.filter(!_.isAtom).count(_ => true)
    val importantFields = List(
      (XMLFields.EVENT_ID.toString, (n \ XMLFields.EVENT_ID.asList).size),
      (XMLFields.EVENT_TIMESTAMP.toString, (n \ XMLFields.EVENT_TIMESTAMP.asList).size))
    importantFields.
      find { case (_, howMany) => howMany < totalElems }.
      map {
        case (fieldName, howMany) =>
          println(s"${howMany} elements (out of ${totalElems}) have ${fieldName}") //  TODO: logger.error
          None
      }.getOrElse(Some(NodeSeqImpl(value = n)))
  }
  private case class NodeSeqImpl(value: xml.Elem) extends Elem
}

