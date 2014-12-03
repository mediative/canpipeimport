package canpipe.parser.spark

import canpipe.parser.FilterRule
import org.apache.spark.rdd.RDD
import canpipe.EventDetail
import spark.util.wrapper.HDFSFileName
import spark.util.xml.{ Parser => SparkParser }
import spark.util.xml.FileStructure

// TODO: replace 'println's by Spark logs writing
class Parser(val filterRules: Set[FilterRule]) {

  def this() = this(Set.empty)

  /**
   * Parses a file containing a full XML.
   * @return an RDD where each line has been parsed from XML
   * @note This function has 2 very strong assumptions:
   *       (1) each record in the XML
   *       is found on each line of the file. For example:
   *       <xml>
   *       <thing>thing1</thing>
   *       <thing>thing2</thing>
   *       </xml>
   *       (2) The XML uses 'root' as delimiters:
   *       <root>
   *       <thing>thing1</thing>
   *       <thing>thing2</thing>
   *       </root>
   *       The only reason I did this is because all Canpipe files *I HAVE SEEN* are like this.
   *       But I have no evidence whatsoever beyong that to suggest that *all* of them are stored that way.
   *       TODO: verify this hypothesis.
   */
  def parse(sc: org.apache.spark.SparkContext, fN: HDFSFileName): RDD[EventDetail] = {
    // TODO: clean syntax
    val rdd = sc.textFile(fN.name)
    val fs = new FileStructure("root", rdd)
    val (t2, rp) = util.Base.timeInMs { SparkParser.parse(fs.lines) }
    println(s"${fN.name}: parsing took ${t2} ms. Generated an RDD of Elem's")
    //println(fN.name + " =============================") // TODO: put this with 'logger.debug'
    rp.flatMap { anXMLNode =>
      canpipe.xml.Elem(anXMLNode).map { canpipeXMLNode =>
        // TODO: use 'filterRules' here
        // TODO: use also the 'headings' and 'directories'
        val ed = canpipe.Tables(canpipeXMLNode).events
        // val (t4, ed) = util.Base.timeInMs { <code to generate data> }  // TODO: put this with 'logger.debug'
        //println(s"\t\t create an EventDetail took ${t4} ms.")
        ed
      }.getOrElse(None)
    }
  }

}

