package canpipe.parser.spark

import canpipe.Tables
import org.apache.spark.rdd.RDD
import spark.util.xml.XMLPiecePerLine
import util.{ Base => BaseUtil }
import spark.util.{ Base => SparkUtil }
import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfterAll, FreeSpec }
import spark.util.Base.HDFS

import scala.util.control.Exception._

class ParserTest extends FreeSpec with BeforeAndAfterAll {

  val nonExistentFileName = util.Base.String.generateRandom(10)
  val testName = "my test"
  var sc: SparkContext = _
  val myParser = new Parser()

  override def beforeAll() {
    assert(!HDFS.fileExists(nonExistentFileName))
    sc = new SparkContext("local[4]", testName)
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.master.port")
  }

  val twoDirsTwoHeadingsEvent =
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

  private[spark] def toOneLiner(e: scala.xml.Elem): scala.xml.Elem = scala.xml.XML.loadString(e.toString.replace("\n", ""))

  "A (spark) Parser" - {
    "should yield an empty result when parsing" - {
      "an XML from an unexistent file" in {
        val rdd = myParser.parse(new XMLPiecePerLine("root", sc.textFile(nonExistentFileName)))
        val theCount = catching(classOf[Exception]).opt { rdd.count() }.getOrElse(0L)
        assert(theCount == 0)
      }

      "an empty XML " in {
        val rdd = myParser.parse(new XMLPiecePerLine(sc, "root", <root></root>))
        assert(rdd.count() == 0)
      }
    }

    "should report the proper number of events when" - {

      "all events have all fields" in {

        // third-event was hand-picked from logs
        val eventsXML =
          <root>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="22fcfb29-b1aa-42f5-89aa-a93b4e8b7884" timestamp="2015-01-13T20:51:42.695-05:00" site="api" siteLanguage="EN" eventType="click" isMap="false" userId="api-ypg-searchapp-android-html5" typeOfLog="click"><apiKey></apiKey><sessionId>577754b58ef174e82</sessionId><transactionDuration>16</transactionDuration><cachingUsed>false</cachingUsed><applicationId>7</applicationId><referrer>http://mobile.yp.ca/bus/Alberta/Lethbridge/Royal-Satellite-Sales-Service/5194731.html?product=L2</referrer><user><id>251790082</id><ip>23.22.220.184</ip><deviceId>MOBILE</deviceId><userAgent>AndroidNativeYP/3.4.1 (Linux; U; Android 2.3.5; en-ca; SGH-T989D Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1</userAgent><browser>MOBILE_SAFARI</browser><browserVersion>4.0</browserVersion><os>ANDROID2</os></user><search><searchId>577754b58ef174e82_R2Fz_Q3VycmVudCBsb2NhdGlvbg_1</searchId><searchBoxUsed>false</searchBoxUsed><disambiguationPopup>N</disambiguationPopup><failedOrSuccess>Success</failedOrSuccess><calledBing>N</calledBing><matchedGeo/><allListingsTypesMainLists></allListingsTypesMainLists><relatedListingsReturned>false</relatedListingsReturned><allHeadings><heading><name>01157630</name><category>B</category></heading><heading><name>01157800</name><category>B</category></heading></allHeadings><type>merchant</type><radius_1>0.0</radius_1><radius_2>0.0</radius_2><merchants id="5194731" zone="" latitude="49.662243" longitude="-112.8771"><isListingRelevant>true</isListingRelevant><entry><headings><categories>01157630,01157800</categories></headings><directories><channel1>085750</channel1></directories><listingType>L2</listingType></entry></merchants><searchAnalysis><fuzzy>false</fuzzy><geoExpanded>false</geoExpanded><businessName>false</businessName></searchAnalysis></search><requestUri>/merchant/5194731?what=Gas&amp;where=Current+location&amp;fmt=JSON&amp;ypid=7&amp;sessionid=577754b58ef174e82</requestUri><searchAnalytics><entry key="refinements" value=""/><entry key="geoPolygons" value=""/><entry key="geoProvince" value=""/><entry key="geoDirectories" value="092170"/><entry key="collatedWhat" value=""/><entry key="relevantHeadings" value="00617200,01186800,00924800,00618075,00901275"/><entry key="geoCentroid" value="43.9328429,-78.6967274"/><entry key="geoType" value="X"/><entry key="collatedWhere" value=""/><entry key="geoName" value="Current location"/></searchAnalytics></Event>
          </root>

        val rdd = myParser.parse(new XMLPiecePerLine(sc, "root", eventsXML))
        assert(rdd.count() == 3)
      }

      "some events miss id's" in {
        val eventsXML =
          <root>
            <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
          </root>

        val rdd = myParser.parse(new XMLPiecePerLine(sc, "root", eventsXML))
        assert(rdd.count() == 1)
      }

      "some events miss timestamps" in {
        val eventsXML =
          <root>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
          </root>

        val rdd = myParser.parse(new XMLPiecePerLine(sc, "root", eventsXML))
        val theCount = rdd.count()
        assert(theCount == 1)
      }

      "parsing 3 different types of events (impression, business detail, navigational click)" in {
        val rddF = sc.textFile(getClass.getResource("/CanPipeExample.xml").getPath)
        val rdd = myParser.parse(new XMLPiecePerLine("root", rddF)).groupBy(_.events.map(_.basicInfo.id))
        assert(rdd.count() == 3)
      }
    }

    "should generate the right amount of headings" in {
      val rddResults = myParser.parse(new XMLPiecePerLine(sc, "root", toOneLiner(twoDirsTwoHeadingsEvent)))
      withClue(s"Event not generated") { assert(rddResults.map(_.events).collect().head.isDefined) }
      val headings = rddResults.map(_.headings).collect().head
      assert(headings.size == 2)
    }

    "should generate the right amount of directories" in {
      val rddResults = myParser.parse(new XMLPiecePerLine(sc, "root", toOneLiner(twoDirsTwoHeadingsEvent)))
      withClue(s"Event not generated") { assert(rddResults.map(_.events).collect().head.isDefined) }
      val directories = rddResults.map(_.directories).collect().head
      assert(directories.size == 2)
    }

    "when processing real Canpipe files should" - {
      import util.wrapper.{ String => StringWrapper }
      case class ResourceFileName(value: String) extends StringWrapper {
        def absoluteFileName = getClass.getResource(s"/${value}").getFile
        override def toString = s"Resource file '${value}'"
      }
      case class EventsFileWithCount(name: ResourceFileName, eventsItContains: Long)
      val resourceFileNamesAndNumberOfEvents: List[(EventsFileWithCount, Int)] = List(
        (EventsFileWithCount(ResourceFileName("sample.5000.xml"), 5000), 5),
        (EventsFileWithCount(ResourceFileName("sample.50.xml"), 50), 15))

      case class EventsParsed(absoluteFileName: String, result: RDD[Tables], expectedEvents: Long, parsingTimes: Set[Long])

      // NB: 'lazy' evaluation, as it uses 'sc', which is only initialized when tests start to run.
      lazy val rddsAndExpectedNumberOfEvents: Set[EventsParsed] = Set(
        (EventsFileWithCount(ResourceFileName("sample.5000.xml"), 5000), 5),
        (EventsFileWithCount(ResourceFileName("sample.50.xml"), 50), 15)).map {
          case (fileWithCount, n) =>
            val structured = new XMLPiecePerLine("root", sc.textFile(fileWithCount.name.absoluteFileName))
            val (msToParse, rdd) = BaseUtil.timeInMs {
              myParser.parse(structured)
            }.run
            val parsingTimes = (1 to n - 1).foldLeft(Set[Long](msToParse)) { (allTimes, _) =>
              val (msToParse, _) = BaseUtil.timeInMs {
                myParser.parse(structured)
              }.run
              allTimes + msToParse
            }
            EventsParsed(absoluteFileName = fileWithCount.name.absoluteFileName, result = rdd, expectedEvents = fileWithCount.eventsItContains, parsingTimes)
        }

      "find less than 0.5% of events of errors in data" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, rdd, expectedEvents, parsingTimes) =>
            val rddOfEvents = rdd.flatMap(_.events)
            val howManyEvents = rddOfEvents.count()
            val lowerLimit = expectedEvents * .995
            withClue(s"File parsed: '${fileName}', lower limit = ${lowerLimit}") { assert(howManyEvents >= lowerLimit) }
            // I *should* have unique events on that file. Is that true??
            val howManyUniqueEvents = rddOfEvents.collect().groupBy(_.basicInfo.id).keySet.size
            if (howManyUniqueEvents != howManyEvents) {
              info(s"**** [WARNING] ==> *Unique* events wrong: there are ${howManyEvents} events total, but only ${howManyUniqueEvents} unique ****")
            }
        }
      }

      "be 'fast enough'" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, _, _, parsingTimes) =>
            withClue(s"'${fileName}'the 'parse' took ${parsingTimes.toString()} ms. to run. SHOULD BE FAST, ONLY RDDs INVOLVED!!") {
              assert(parsingTimes.forall(_ < 50))
            }
        }
      }

      "save results 'fast enough'" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, rdd, expectedEvents, parsingTimes) =>
            val txtFileName = s"${BaseUtil.String.generateRandom(12)}.parquet"
            try {
              val (msToWrite, _) = BaseUtil.timeInMs {
                // TODO: next line should be 'saveAsParquetFile', but it doesn't run properly on my test env (Windows).
                rdd.saveAsTextFile(txtFileName) // speed ~ to 'saveAsParquetFile(parquetFileName)' TODO: is this true? How, statistically?
              }.run
              val theCount = rdd.count()
              info(s"'${fileName}' (${expectedEvents} events) generated text file with ${theCount} rows in ${msToWrite} ms . ====> THIS IS A LOWER BOUND FOR THE GENERATION OF A PARQUET FILE OF *THAT* SIZE <====")
            } finally {
              SparkUtil.HDFS.rm(txtFileName)
            }
        }
      }

      "not report events with empty eventId's" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, rdd, expectedEvents, parsingTimes) =>
            val rddOfEvents = rdd.flatMap(_.events)
            val howManyEmptyEventIds = rddOfEvents.filter(_.basicInfo.id.trim.isEmpty).count()
            assert(howManyEmptyEventIds == 0)
        }
      }

      "not report events with invalid timestamps" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, rdd, expectedEvents, parsingTimes) =>
            val rddOfEvents = rdd.flatMap(_.events)
            val howManyEmptyTimestamps = rddOfEvents.filter(e => e.basicInfo.timestamp.trim.isEmpty).count()
            withClue(s"${fileName}: 'eventTimestamp's invalid") { assert(howManyEmptyTimestamps == 0) }
            val howManyInvalidTimestamps = rddOfEvents.filter(e => e.basicInfo.timestampId.value < 0).count()
            withClue(s"${fileName}: 'timestampId's invalid") { assert(howManyInvalidTimestamps == 0) }
        }
      }

      "yield headings with proper category, at least 75% of the time" in {
        rddsAndExpectedNumberOfEvents.foreach {
          case EventsParsed(fileName, rdd, expectedEvents, parsingTimes) =>
            val rddOfHeadings = rdd.flatMap(_.headings)
            val howManyEmptyCategoryIds = rddOfHeadings.filter(_.category.trim.isEmpty).count()
            val propOfNonEmpties = 100 - (((howManyEmptyCategoryIds * 100): Double) / expectedEvents)
            if (howManyEmptyCategoryIds > 0)
              info(s" ====> There are ${howManyEmptyCategoryIds} (out of ${expectedEvents}) events without category on heading on ${fileName}")
            withClue(s"On ${fileName}") { assert(propOfNonEmpties > 75) }
        }
      }

    }
  }

}

// end of file
