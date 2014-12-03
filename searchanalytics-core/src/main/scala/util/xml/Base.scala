package util.xml

object Base {

  implicit def NodeSeq2Helper(n: xml.NodeSeq) = new NodeSeqHelper(n)
  class NodeSeqHelper(n: xml.NodeSeq) {
    def \(thePath: List[String]): List[String] = {
      thePath.length match {
        case 0 => List.empty
        case 1 => (n \ thePath.head).iterator.toList.map(_.text)
        case _ => (n \ thePath.head).iterator.toList.flatMap(n => n \ thePath.tail)
      }
    }
  }

  /*
  def \(n: scala.xml.NodeSeq, thePath: List[String]): List[String] = {
    thePath.length match {
      case 0 => List.empty
      case 1 => (n \ thePath.head).iterator.toList.map(_.text)
      case _ => (n \ thePath.head).iterator.toList.flatMap(n => \(n, thePath.tail))
    }
  }
  */

}
