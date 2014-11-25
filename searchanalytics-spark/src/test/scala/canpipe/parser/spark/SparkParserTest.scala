package canpipe.parser.spark

import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import canpipe.parser.Base.{ CanPipeParser => BasicParser }
import canpipe.parser.spark.{ Parser => SparkParser }

class SparkParserTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  case class fileFromResources(name: String, eventsItContains: Long)
  val resourceFileNamesAndNumberOfEvents: List[(fileFromResources, Int)] = List(
    (fileFromResources("sample.5000.xml", 5000), 5), (fileFromResources("sample.50.xml", 50), 15))

  val resourceFileName50 = "sample.50.xml"

  // TODO: the hard-coding sucks
  private def resourceFileName2HDFSFileName(fileName: String) = s"C:/Users/ldacost1/dev/scala/searchdataimport/searchanalytics-core/src/main/resources/${fileName}"

  "Getting events from an HDFS file" should "count the correct number of lines" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    resourceFileNamesAndNumberOfEvents.foreach {
      case (fileInfo, _) =>
        val hdfsFileName = resourceFileName2HDFSFileName(fileInfo.name)
        val evGroups = EventGroupFromHDFSFile(sc, hdfsFileName = hdfsFileName)
        assert(evGroups.eventsAsString.count() == fileInfo.eventsItContains)
    }

    sc.stop()
    System.clearProperty("spark.master.port")

  }

  s"Result of parsing a CanPipe XML to RDD" should "yield the same number of records as parsing 'normally' * number of headings in events" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myBasicParser = new BasicParser()
    val myParser = new SparkParser()

    resourceFileNamesAndNumberOfEvents.foreach {
      case (fileInfo, _) =>
        info(s"Testing ${fileInfo.name}")
        val hdfsFileName = resourceFileName2HDFSFileName(fileInfo.name)
        val r = myParser.parseEventGroup(events = EventGroupFromHDFSFile(sc, hdfsFileName))
        val eventsInRDD = r.count()
        withClue(s"Result is empty ") { assert(eventsInRDD > 0) }
        val setOfEvents = myBasicParser.parseFromResources(fileInfo.name)
        val howManyExpectedInRDD =
          setOfEvents.foldLeft(0L) { (count, resultForAnEvent) =>
            count + resultForAnEvent.get("/root/Event/search/allHeadings/heading/name").getOrElse(List("")).length
          }
        withClue(s"Expected ${howManyExpectedInRDD}, found ${eventsInRDD}") { assert(eventsInRDD == howManyExpectedInRDD) }
    }

    sc.stop()
    System.clearProperty("spark.master.port")
  }

}

// end of file
