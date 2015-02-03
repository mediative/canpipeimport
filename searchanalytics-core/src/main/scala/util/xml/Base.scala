package util.xml

object Base {

  implicit def NodeSeq2Helper(n: xml.NodeSeq) = new NodeSeqHelper(n)
  class NodeSeqHelper(n: xml.NodeSeq) {
    def \(thePath: List[String]): Seq[String] = {
      // TODO: the check for emptiness *may* be avoided if instead of '.tail' we use '.drop(1)'. Try it.
      if (thePath.isEmpty) Seq.empty
      else {
        thePath.tail.foldLeft((n \ thePath.head)) { (r, x) =>
          r.flatMap(_ \ x)
        }.map(_.text)
      }
    }
  }

  // Structure of a valid (XML) line
  val lineRX = "<([^( |>)]*)([^>]*)?>(.*)<([^ ]*)>".r // TODO: how do you match a forward slash? (I tried \/, didn't work)

  def isValidEntry(aLine: String): Boolean = {
    try {
      val lineRX(tag1, _, label, tag2) = aLine.trim
      (tag2(0) == '/') && (tag1 == tag2.substring(1))
    } catch {
      case e: Exception => false // didn't match the regex
    }
  }

}
