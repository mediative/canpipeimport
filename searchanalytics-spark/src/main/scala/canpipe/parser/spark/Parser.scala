package canpipe.parser.spark

import canpipe.parser.FilterRule
import org.apache.spark.rdd.RDD
import spark.util.xml.{ Parser => SparkParser }
import spark.util.xml.XMLPiecePerLine
import util.Logging

class Parser(val filterRules: Set[FilterRule]) extends Logging with Serializable {

  def this() = this(Set.empty)

  /**
   * Parses a file containing a full XML.
   */
  def parse(fs: XMLPiecePerLine): RDD[EventDetail] = {
    val (t2, rp) = util.Base.timeInMs { SparkParser.parse(fs.wrappedLines) }.run
    logger.debug(s"parsing took ${t2} ms. Generated an RDD of Elem's")
    logger.debug(" =============================")
    rp.flatMap { anXMLNode =>
      canpipe.xml.Elem(anXMLNode).map { canpipeXMLNode =>
        // TODO: use 'filterRules' here
        EventDetail(canpipeXMLNode)
      }.getOrElse(None)
    }
  }

}

