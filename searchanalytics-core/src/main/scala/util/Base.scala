package util

// import com.typesafe.scalalogging.Logging
// import com.typesafe.scalalogging.slf4j.StrictLogging

object Base { // extends Logging {

  def getCurrentTimeStamp: java.sql.Timestamp = {
    new java.sql.Timestamp(java.util.Calendar.getInstance().getTime.getTime)
  }
  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  object String {

    def generateRandom(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString("")

  }

  object XML {
    // TODO: I reckon this can be implemented using a Stack (http://www.scala-lang.org/api/current/index.html#scala.collection.mutable.Stack)
    case class XPath(s: String) {
      def asString = s
    }
    object XPath {
      private[util] val SEP = "/"
      private[util] def fromReverseNameList(reverseNameList: List[String]): String =
        reverseNameList.reverse.mkString(start = SEP, sep = SEP, end = "")
      private[util] def toReverseNameList(s: String): List[String] = {
        { if (s.startsWith(SEP)) s.substring(1) else s }.split(SEP).reverse.toList
      }
      // creates a 'root'
      def fromRoot(): XPath = fromRoot("root")
      def fromRoot(rootName: String): XPath = new XPath(SEP + rootName)
      // def apply(s: String): XPath = { if (s.startsWith(SEP)) (new XPath(s)) else (new XPath(SEP + s)) }
      def add(aPath: XPath, aPart: String) = XPath(s"${aPath.s}${SEP}${aPart}")
      def removeLast(aPath: XPath) = {
        XPath(fromReverseNameList(toReverseNameList(aPath.s).tail))
      }
    }
  }

}
