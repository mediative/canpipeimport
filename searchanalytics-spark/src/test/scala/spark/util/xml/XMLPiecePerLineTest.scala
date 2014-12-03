package spark.util.xml

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfter, FreeSpec }
import spark.util.Base.HDFS

class XMLPiecePerLineTest extends FreeSpec with BeforeAndAfter {

  val nonExistentFileName = util.Base.String.generateRandom(10)
  val testName = "my test"
  var sc: SparkContext = _

  before {
    sc = new SparkContext("local[4]", testName)
    assert(!HDFS.fileExists(nonExistentFileName))
  }

  after {
    sc.stop()
    System.clearProperty("spark.master.port")
  }

  // Returns (number-of-lines, number-of-wrapped-lines)
  private def createAndCount(root: String, block: => RDD[String]): (Long, Long) = {
    val fs = new XMLPiecePerLine(root, block)
    (fs.lines.count(), fs.wrappedLines.count())
  }

  private def toRDD(e: scala.xml.Elem): RDD[String] = {
    sc.parallelize(e.toString().split("\n"))
  }

  val noEventsXML =
    <root>
    </root>

  val threeDifferentEventsXML =
    <root>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb842" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
    </root>

  val twoIdenticalEventsXML =
    <root>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
    </root>

  "An XMLPiecePerLine" - {

    "contains 0 lines when populated from" - {
      "an empty RDD" in {
        val (numLines, numWrappedLines) = createAndCount("root", sc.emptyRDD[String])
        withClue("Incorrect number of lines") { assert(numLines === 0) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
      "an RDD where all entries are invalid" in {
        val (numLines, numWrappedLines) = createAndCount("root", sc.parallelize(List("hello")))
        withClue("Incorrect number of lines") { assert(numLines === 0) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
      "an RDD whose only entries are header and footer" in {
        val (numLines, numWrappedLines) = createAndCount("root", sc.parallelize(List("<root>", "</root>")))
        withClue("Incorrect number of lines") { assert(numLines === 0) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
      "an empty XML" in {
        val (numLines, numWrappedLines) = createAndCount("root", toRDD(noEventsXML))
        withClue("Incorrect number of lines") { assert(numLines === 0) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
    }

    "contains the same amount of lines as well-formed lines when source is" - {
      // TODO: combine the tests below with a scalacheck generator to make them general
      "an RDD" in {
        val (numLines, numWrappedLines) = createAndCount("root", sc.parallelize(List("<root>", "<entry>luis</entry>", "</root>")))
        withClue("Incorrect number of lines") { assert(numLines === 1) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
      "an XML with 3 different entries" in {
        val (numLines, numWrappedLines) = createAndCount("root", toRDD(threeDifferentEventsXML))
        withClue("Incorrect number of lines") { assert(numLines === 3) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
      "an XML with 2 identical entries" in {
        val (numLines, numWrappedLines) = createAndCount("root", toRDD(twoIdenticalEventsXML))
        withClue("Incorrect number of lines") { assert(numLines === 2) }
        withClue("Number of wrapped lines do not correspond with number of lines") { assert(numLines === numWrappedLines) }
      }
    }

  }

}
