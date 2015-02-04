package canpipe.parser.spark

import spark.CanpipeFileName
import spark.util.xml.XMLPiecePerLine
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
      // TODO: clean syntax
      val rddTF = sc.textFile(nonExistentFileName)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rdd = myParser.parse(fs)
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
      // TODO: clean syntax
      val rddTF = sc.textFile(fileNameCreated)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rdd = myParser.parse(fs)
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
      // TODO: clean syntax
      val rddTF = sc.textFile(fileNameCreated)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rdd = myParser.parse(fs)
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
      // TODO: clean syntax
      val rddTF = sc.textFile(fileNameCreated)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rdd = myParser.parse(fs)
      val theCount = rdd.count()
      assert(theCount == 1)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "parse an event hand-picked from logs" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()

    val eventsXML =
      <root>
        <Event id="22fcfb29-b1aa-42f5-89aa-a93b4e8b7884" timestamp="2015-01-13T20:51:42.695-05:00" site="api" siteLanguage="EN" eventType="click" isMap="false" userId="api-ypg-searchapp-android-html5" typeOfLog="click"><apiKey></apiKey><sessionId>577754b58ef174e82</sessionId><transactionDuration>16</transactionDuration><cachingUsed>false</cachingUsed><applicationId>7</applicationId><referrer>http://mobile.yp.ca/bus/Alberta/Lethbridge/Royal-Satellite-Sales-Service/5194731.html?product=L2</referrer><user><id>251790082</id><ip>23.22.220.184</ip><deviceId>MOBILE</deviceId><userAgent>AndroidNativeYP/3.4.1 (Linux; U; Android 2.3.5; en-ca; SGH-T989D Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1</userAgent><browser>MOBILE_SAFARI</browser><browserVersion>4.0</browserVersion><os>ANDROID2</os></user><search><searchId>577754b58ef174e82_R2Fz_Q3VycmVudCBsb2NhdGlvbg_1</searchId><searchBoxUsed>false</searchBoxUsed><disambiguationPopup>N</disambiguationPopup><failedOrSuccess>Success</failedOrSuccess><calledBing>N</calledBing><matchedGeo/><allListingsTypesMainLists></allListingsTypesMainLists><relatedListingsReturned>false</relatedListingsReturned><allHeadings><heading><name>01157630</name><category>B</category></heading><heading><name>01157800</name><category>B</category></heading></allHeadings><type>merchant</type><radius_1>0.0</radius_1><radius_2>0.0</radius_2><merchants id="5194731" zone="" latitude="49.662243" longitude="-112.8771"><isListingRelevant>true</isListingRelevant><entry><headings><categories>01157630,01157800</categories></headings><directories><channel1>085750</channel1></directories><listingType>L2</listingType></entry></merchants><searchAnalysis><fuzzy>false</fuzzy><geoExpanded>false</geoExpanded><businessName>false</businessName></searchAnalysis></search><requestUri>/merchant/5194731?what=Gas&amp;where=Current+location&amp;fmt=JSON&amp;ypid=7&amp;sessionid=577754b58ef174e82</requestUri><searchAnalytics><entry key="refinements" value=""/><entry key="geoPolygons" value=""/><entry key="geoProvince" value=""/><entry key="geoDirectories" value="092170"/><entry key="collatedWhat" value=""/><entry key="relevantHeadings" value="00617200,01186800,00924800,00618075,00901275"/><entry key="geoCentroid" value="43.9328429,-78.6967274"/><entry key="geoType" value="X"/><entry key="collatedWhere" value=""/><entry key="geoName" value="Current location"/></searchAnalytics></Event>
      </root>
    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"

    HDFS.writeToFile(fileName = fileNameCreated, eventsXML.toString())
    try {
      val rddTF = sc.textFile(fileNameCreated)
      val rdd = myParser.parse(new XMLPiecePerLine("root", rddTF)).groupBy(_.events.map(_.basicInfo.id))
      val theCount = rdd.count()
      assert(theCount == 1)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "properly parse 3 different types (impression, business detail, navigational click) of events" in {
    import scala.io.Source
    val lines = "<root>\n" +
      BaseUtil.using(Source.fromURL(getClass.getResource("/CanPipeExample.xml"))) { _.getLines().toList }.mkString("\n") + // path relative to the resources directory
      "\n</root>"
    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"
    HDFS.writeToFile(fileName = fileNameCreated, lines)
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val myParser = new Parser()
    try {
      val rddF = sc.textFile(fileNameCreated)
      val rdd = myParser.parse(new XMLPiecePerLine("root", rddF)).groupBy(_.events.map(_.basicInfo.id))
      val theCount = rdd.count()
      assert(theCount == 3)
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
  it should "properly generate the right amount of headings" in {
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
      // TODO: clean syntax
      val rddTF = sc.textFile(fileNameCreated)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rddOfSetOfHeadings = myParser.parse(fs).map(_.headings)
      assert(rddOfSetOfHeadings.collect().head.size == 2)
    } finally {
      HDFS.rm(fileNameCreated)
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "properly generate the right amount of directories" in {
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
      // TODO: clean syntax
      val rddTF = sc.textFile(fileNameCreated)
      val fs = new XMLPiecePerLine("root", rddTF)
      val rddOfSetOfHeadings = myParser.parse(fs).map(_.directories)
      assert(rddOfSetOfHeadings.collect().head.size == 2)
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
    (EventsFileWithCount(ResourceFileName("sample.5000.xml"), 5000), 5),
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
                  // TODO: clean syntax
                  val rddTF = sc.textFile(fileName)
                  val fs = new XMLPiecePerLine("root", rddTF)
                  myParser.parse(fs)
                }.run
                withClue(s"'${fileInfo.name}'the 'parse' took ${msToParse} ms. to run. SHOULD BE FAST, ONLY RDDs INVOLVED!!") {
                  assert(msToParse < 50)
                }
                // TODO: next line should be 'saveAsParquetFile', but it doesn't run properly on my test env (Windows).
                rdd.saveAsTextFile(txtFileName) // speed ~ to 'saveAsParquetFile(parquetFileName)' TODO: is this true? How, statistically?
                rdd
              }.run
            val theCount = rdd.count()
            info(s"'${fileInfo.name}' (${fileInfo.eventsItContains} events) generated text file with ${theCount} rows in ${msToWrite} ms . ====> THIS IS A LOWER BOUND FOR THE GENERATION OF A PARQUET FILE OF *THAT* SIZE <====")
            val rddOfEvents = rdd.flatMap(_.events)
            val howManyUniqueEvents = rddOfEvents.collect().groupBy(_.basicInfo.id).keySet.size
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
          // TODO: clean syntax
          val rddTF = sc.textFile(fileName)
          val fs = new XMLPiecePerLine("root", rddTF)
          val rddOfEvents = myParser.parse(fs).flatMap(_.events)
          val howManyEmptyEventIds = rddOfEvents.filter(_.basicInfo.id.trim.isEmpty).count()
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
          // TODO: clean syntax
          val rddTF = sc.textFile(fileName)
          val fs = new XMLPiecePerLine("root", rddTF)
          val rdd = myParser.parse(fs)
          val rddOfEvents = rdd.flatMap(_.events)
          val howManyEmptyTimestamps = rddOfEvents.filter(e => e.basicInfo.timestamp.trim.isEmpty).count()
          withClue(s"${fileInfo.name}: 'eventTimestamp's invalid") { assert(howManyEmptyTimestamps == 0) }
          val howManyInvalidTimestamps = rddOfEvents.filter(e => e.basicInfo.timestampId.value < 0).count()
          withClue(s"${fileInfo.name}: 'timestampId's invalid") { assert(howManyInvalidTimestamps == 0) }
      }
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

}

// end of file
