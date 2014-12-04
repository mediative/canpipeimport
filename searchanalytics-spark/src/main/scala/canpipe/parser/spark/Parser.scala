package canpipe.parser.spark

import canpipe.parser.FilterRule
import org.apache.spark.rdd.RDD
import canpipe.Tables
import spark.util.wrapper.HDFSFileName
import spark.util.xml.{ Parser => SparkParser }
import spark.util.xml.XMLPiecePerLine
import util.Logging

import scala.xml.Elem

class Parser(val filterRules: Set[FilterRule]) extends Logging with Serializable {

  def this() = this(Set.empty)

  /**
   * Parses a file containing a full XML.
   */
  def parse(sc: org.apache.spark.SparkContext, fN: HDFSFileName): RDD[Tables] = {
    // TODO: clean syntax
    val rdd = sc.textFile(fN.name)
    val fs = new XMLPiecePerLine("root", rdd)
    val (t2, rp): (Long, RDD[Elem]) = util.Base.timeInMs { SparkParser.parse(fs.wrappedLines) }.run
    logger.debug(s"parsing took ${t2} ms. Generated an RDD of Elem's")
    logger.debug(" =============================")
    rp.flatMap { anXMLNode =>
      canpipe.xml.Elem(anXMLNode).map { canpipeXMLNode =>
        canpipe.Tables(canpipeXMLNode)
      }
    }
  }

}

