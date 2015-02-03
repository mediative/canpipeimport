package spark.util.xml

import org.apache.spark.rdd.RDD
import util.xml.{ Base => XMLUtil }

/**
 * Assumes that each "piece" of XML comes on a different line (ie, entries in an RDD).
 */
case class XMLPiecePerLine(starter: String, rdd: RDD[String]) extends Structure {

  val lines: RDD[String] = {
    rdd.
      filter(!_.endsWith(starter + ">")). // prevent that in the between the lines there are the ones that contain the root of the XML
      filter { XMLUtil.isValidEntry(_) } // keeps only well-formed lines
  }

}
