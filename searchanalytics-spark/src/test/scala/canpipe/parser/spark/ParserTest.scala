package canpipe.parser.spark

import util.{ Base => BaseUtil }
import spark.util.{ Base => SparkUtil }
import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import spark.util.Base.HDFS
import spark.util.wrapper.HDFSFileName

import scala.util.control.Exception._

class ParserTest extends FlatSpec with BeforeAndAfter {

  val nonExistentFileName = util.Base.String.generateRandom(10)

  before {
    assert(!HDFS.fileExists(nonExistentFileName))
  }

  after {

  }

  "Parsing a file from HDFS" should "yield an empty RDD if file does not exist" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = nonExistentFileName))
      val theCount = catching(classOf[Exception]).opt { rdd.count() }.getOrElse(0L)
      assert(theCount == 0)
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "work if I save and then read" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>

    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"

    HDFS.writeToFile(fileName = fileNameCreated, eventsXML.toString())
    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = fileNameCreated))
      val theCount = rdd.count()
      assert(theCount == 2)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "fail if my events have no id" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val eventsXML =
      <root>
        <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>

    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"

    HDFS.writeToFile(fileName = fileNameCreated, eventsXML.toString())
    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = fileNameCreated))
      val theCount = rdd.count()
      assert(theCount == 1)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "fail if my events have no timestamp" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>

    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"

    HDFS.writeToFile(fileName = fileNameCreated, eventsXML.toString())
    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = fileNameCreated))
      val theCount = rdd.count()
      assert(theCount == 1)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  private[spark] def xmlOneEventPerLine(events: List[scala.xml.Elem]): String = {
    def eventElem2Line(e: scala.xml.Elem) = e.toString.replace("\n", "")
    s"<root>\n${events.map(eventElem2Line).mkString("\n")}\n</root>"
  }
  it should "properly generate as many rows as headings (when no directoy is involved)" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val anEvent =
      <Event id="54de05a7-cc76-4e7b-9244-108b5cfd2962" timestamp="2014-09-30T11:59:59.956-04:00" site="ypg" siteLanguage="EN" eventType="click" isMap="false" typeOfLog="click">
        <search>
          <searchId>272d283c-4905-4f15-8fa1-0371f6a0728f</searchId>
          <allHeadings>
            <heading>
              <name>00304200</name>
              <category>B</category>
            </heading>
            <heading>
              <name>00304201</name>
              <category>A</category>
            </heading>
          </allHeadings>
        </search>
      </Event>
    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"
    HDFS.writeToFile(fileName = fileNameCreated, xmlOneEventPerLine(List(anEvent)))
    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = fileNameCreated))
      val theCount = rdd.count()
      assert(theCount == 2)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "properly generate as many rows as headings * directories" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val anEvent =
      <Event id="54de05a7-cc76-4e7b-9244-108b5cfd2962" timestamp="2014-09-30T11:59:59.956-04:00" site="ypg" siteLanguage="EN" eventType="click" isMap="false" typeOfLog="click">
        <search>
          <searchId>272d283c-4905-4f15-8fa1-0371f6a0728f</searchId>
          <directoriesReturned>095449:Ile De MontrÃ©al - Centre,095447:Ile De MontrÃ©al - Est</directoriesReturned>
          <allHeadings>
            <heading>
              <name>00304200</name>
              <category>B</category>
            </heading>
            <heading>
              <name>00304201</name>
              <category>A</category>
            </heading>
          </allHeadings>
        </search>
      </Event>
    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"
    HDFS.writeToFile(fileName = fileNameCreated, xmlOneEventPerLine(List(anEvent)))
    try {
      val rdd = myParser.parse(sc, HDFSFileName(name = fileNameCreated))
      val theCount = rdd.count()
      assert(theCount == 2 * 2) // headings * directories
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  import util.wrapper.{ String => StringWrapper }
  case class ResourceFileName(value: String) extends StringWrapper {
    def absoluteFileName = getClass.getResource(s"/${value}").getFile
    override def toString = s"Resource file '${value}'"
  }
  case class EventsFileWithCount(name: ResourceFileName, eventsItContains: Long)
  val resourceFileNamesAndNumberOfEvents: List[(EventsFileWithCount, Int)] = List(
    // (EventsFileWithCount(ResourceFileName("sample.5000.xml"), 5000), 5),
    (EventsFileWithCount(ResourceFileName("sample.50.xml"), 50), 15))

  "Parsing valid Canpipe files from HDFS" should "yield the right number of events (fast enough, por favor)" in {
    // NB: this test also reports on 'writing to file' speed.
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    try {
      resourceFileNamesAndNumberOfEvents.foreach {
        case (fileInfo, _) =>
          val fileName = fileInfo.name.absoluteFileName
          val txtFileName = s"${BaseUtil.String.generateRandom(12)}.parquet"
          try {
            val (msToWrite, rdd) =
              BaseUtil.timeInMs {
                val (msToParse, rdd) = BaseUtil.timeInMs {
                  myParser.parse(sc, HDFSFileName(name = fileName))
                }
                withClue(s"'${fileInfo.name}'the 'parse' took ${msToParse} ms. to run. SHOULD BE FAST, ONLY RDDs INVOLVED!!") {
                  assert(msToParse < 50)
                }
                // TODO: next line should be 'saveAsParquetFile', but it doesn't run properly on my test env (Windows).
                rdd.saveAsTextFile(txtFileName) // speed ~ to 'saveAsParquetFile(parquetFileName)' TODO: is this true? How, statistically?
                rdd
              }
            val theCount = rdd.count()
            info(s"'${fileInfo.name}' (${fileInfo.eventsItContains} events) generated text file with ${theCount} rows in ${msToWrite} ms . THIS IS A LOWER BOUND FOR THE GENERATION OF A PARQUET FILE OF *THAT* SIZE <======================")
            val howManyUniqueEvents = rdd.collect().groupBy(_.eventId).keySet.size
            assert(howManyUniqueEvents == fileInfo.eventsItContains)
          } finally {
            SparkUtil.HDFS.rm(txtFileName)
          }
      }
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "yield events with proper eventId" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    try {
      resourceFileNamesAndNumberOfEvents.foreach {
        case (fileInfo, howManyEvents) =>
          val fileName = fileInfo.name.absoluteFileName
          val rdd = myParser.parse(sc, HDFSFileName(name = fileName))
          val howManyEmptyEventIds = rdd.filter(_.eventId.trim.isEmpty).count()
          assert(howManyEmptyEventIds == 0)
      }
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "yield events with proper timestamp" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    try {
      resourceFileNamesAndNumberOfEvents.foreach {
        case (fileInfo, howManyEvents) =>
          val fileName = fileInfo.name.absoluteFileName
          val rdd = myParser.parse(sc, HDFSFileName(name = fileName))
          val howManyEmptyTimestamps = rdd.filter(e => e.eventTimestamp.trim.isEmpty).count()
          withClue("'eventTimestamp's invalid") { assert(howManyEmptyTimestamps == 0) }
          val howManyInvalidTimestamps = rdd.filter(e => e.timestampId < 0).count()
          withClue("'timestampId's invalid") { assert(howManyInvalidTimestamps == 0) }
      }
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

}

// end of file
