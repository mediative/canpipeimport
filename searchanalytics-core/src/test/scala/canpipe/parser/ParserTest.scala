package canpipe.parser

import java.io.File

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import canpipe.parser.Base.CanPipeParser
import util.{ Base => BaseUtil }

/*
A complete XML event:
      <root>
        <Event id="54de05a7-cc76-4e7b-9244-108b5cfd2962" timestamp="2014-09-30T11:59:59.956-04:00" site="ypg" siteLanguage="EN" eventType="click" isMap="false" typeOfLog="click">
          <apiKey></apiKey>
          <sessionId>272d283c-4905-4f15-8fa1-0371f6a0728f</sessionId>
          <transactionDuration>204</transactionDuration>
          <cachingUsed>false</cachingUsed>
          <user>
            <id>50990366</id>
            <ip>24.212.244.24</ip>
            <deviceId>COMPUTER</deviceId>
            <userAgent>Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0</userAgent>
            <browser>FIREFOX3</browser>
            <browserVersion>32.0</browserVersion>
            <os>MAC_OS_X</os>
          </user>
          <search>
            <searchId>272d283c-4905-4f15-8fa1-0371f6a0728f</searchId>
            <searchBoxUsed>false</searchBoxUsed>
            <disambiguationPopup>N</disambiguationPopup>
            <failedOrSuccess>Success</failedOrSuccess>
            <calledBing>N</calledBing>
            <matchedGeo/>
            <allListingsTypesMainLists></allListingsTypesMainLists>
            <relatedListingsReturned>false</relatedListingsReturned>
            <allHeadings>
              <heading>
                <name>00304200</name>
                <category>B</category>
              </heading>
            </allHeadings>
            <type>merchant</type>
            <radius_1>0.0</radius_1>
            <radius_2>0.0</radius_2>
            <merchants id="7521308" zone="" latitude="45.48856" longitude="-75.60643">
              <isListingRelevant>true</isListingRelevant>
              <entry>
                <headings>
                  <categories>00304200</categories>
                </headings>
                <directories>
                  <channel1>092740</channel1>
                </directories>
                <listingType>NONE</listingType>
              </entry>
            </merchants>
            <searchAnalysis>
              <fuzzy>false</fuzzy>
              <geoExpanded>false</geoExpanded>
              <businessName>false</businessName>
            </searchAnalysis>
          </search>
          <pageName>merchant</pageName>
          <requestUri>/bus/Quebec/Gatineau/Centre-Medical-du-Portage/7521308.html</requestUri>
          <searchAnalytics>
            <entry key="refinements" value=""/>
          </searchAnalytics>
        </Event>
      </root>

 */
class ParserTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  // TODO: put that in Utils
  private def timeInMs[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  // // ("/sample.50.xml"))) // ("/sample1Impression.xml"))) //  // ("/analytics.log.2014-06-01-15")))

  private def getAllTimesButSlowest(myParser: CanPipeParser, resourceFileName: String, howManyRuns: Int): Seq[Long] = {
    (1 to howManyRuns).
      map { run => val (timeItTook, _) = timeInMs(myParser.parseFromResources(resourceFileName)); timeItTook }.
      sortWith((t1, t2) => t1 > t2).
      tail
  }

  // http://alvinalexander.com/scala/scala-save-write-xml-data-string-to-file
  // returns (result.size, headings.size)
  def parseAndCountResult(eventsXML: scala.xml.Elem): Int = {
    testParse(eventsXML: scala.xml.Elem).size
  }

  // (result, headings)
  private def testParse(eventsXML: scala.xml.Elem) = {
    // create a temp file to save XML
    val fileName = BaseUtil.String.generateRandom(10) + ".xml"
    (new File(fileName)).delete() // just in case it exists
    // save it:
    scala.xml.XML.save(fileName, eventsXML, "UTF-8", true, null)
    // parse it:
    val myParser = new CanPipeParser()
    try {
      BaseUtil.using(scala.io.Source.fromFile(fileName)) {
        theSource => myParser.parse(source = theSource)
      }
    } finally {
      // clean up
      (new File(fileName)).delete()
    }
  }

  "Parsing of an XML" should "yield an empty structure when XML is empty" in {
    val emptyXML = <root></root>
    val resultSize = parseAndCountResult(emptyXML)
    withClue(s"Result has ${resultSize} entries") { assert(resultSize == 0) }
  }

  it should "parse the right amount of (simple) events" in {
    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>
    val resultSize = parseAndCountResult(eventsXML)
    withClue(s"Result has ${resultSize} entries") { assert(resultSize == 2) }
  }

  it should "not parse headings, if there isn't any " in {
    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>
    val numHeadingsPerEvent = testParse(eventsXML).map(_.get("/root/Event/search/allHeadings/heading/name").getOrElse(List.empty).length)
    withClue(s"num heading per event should be 0 everywhere, but it is: ${numHeadingsPerEvent.mkString(start = "[", sep = ",", end = "]")}") { numHeadingsPerEvent.forall(_ == 0) }
  }

  it should "parse the right amount of headings" in {
    val eventsXML =
      <root>
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
      </root>
    val setOfResults = testParse(eventsXML)
    assert(setOfResults.size == 1)
    val headingsInFirstEvent = setOfResults.head.get("/root/Event/search/allHeadings/heading/name").getOrElse(List.empty)
    withClue(s"num heading per ONLY event present should be 2, but it is: ${headingsInFirstEvent.length}") { assert(headingsInFirstEvent.length == 2) }
  }

  it should "parse the right amount of categories for headings than headings" in {
    val eventsXML =
      <root>
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
      </root>
    val setOfResults = testParse(eventsXML)
    assert(setOfResults.size == 1)
    val numHeadingsPerEvent = setOfResults.map(_.get("/root/Event/search/allHeadings/heading/name").getOrElse(List.empty).length)
    withClue(s"num heading per ONLY event present should be 2, but it is: ${numHeadingsPerEvent.mkString(start = "[", sep = ",", end = "]")}") { assert(numHeadingsPerEvent.forall(_ == 2)) }
    val numHeadingsCategories = setOfResults.map(_.get("/root/Event/search/allHeadings/heading/category").getOrElse(List.empty).length)
    withClue(s"num heading categories should be 2, but it is: ${numHeadingsCategories.mkString(start = "[", sep = ",", end = "]")}") { assert(numHeadingsCategories.forall(_ == 2)) }
  }

  it should "parse the right values of categories for headings than headings" in {
    val eventsXML =
      <root>
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
      </root>
    val setOfResults = testParse(eventsXML)
    assert(setOfResults.size == 1)
    val firstEvent = setOfResults.head
    val listOfHeadings = firstEvent.get("/root/Event/search/allHeadings/heading/name").getOrElse(List.empty)
    val numHeadings = listOfHeadings.length
    withClue(s"num heading per ONLY event present should be 2, but it is: ${numHeadings}") { assert(numHeadings == 2) }
    info(s"listOfHeadings = ${listOfHeadings.mkString(start = "{", sep = ",", end = "}")}, length = ${numHeadings}")
    assert(listOfHeadings.map(_.toLong).toSet == Set("00304200", "00304201").map(_.toLong))
    val headingsCategories = firstEvent.get("/root/Event/search/allHeadings/heading/category").getOrElse(List.empty)
    withClue(s"num heading categories should be 2, but it is: ${headingsCategories.length}") { assert(headingsCategories.length == 2) }
  }

  it should "generate the correct timestamp and its 'foreign-key', as the timestamp is valid" in {
    val eventsXML =
      <root>
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
      </root>
    val setOfResults = testParse(eventsXML)
    assert(setOfResults.size == 1)
    val firstEvent = setOfResults.head
    withClue("No timestamp entry") { assert(firstEvent.get("/root/Event/@timestamp").isDefined) }
    withClue("No timestamp FK entry") { assert(firstEvent.get("/root/Event/timestampId").isDefined) }
  }

  case class fileFromResources(name: String, eventsItContains: Long)

  val resourceFileNamesAndNumberOfEvents: List[(fileFromResources, Int)] = List(
    (fileFromResources("sample.5000.xml", 5000), 5), (fileFromResources("sample.50.xml", 50), 15))

  // 'n' events MUST take <= n * speedFactor ms.
  val maxTimePerEventInMs = 1.25 // TODO: too big!!!
  s"Parsing of 'n' events from a CanPipe XML" should s"take less than '${maxTimePerEventInMs} * n' ms. (on average) " in {
    val myParser = new CanPipeParser()
    resourceFileNamesAndNumberOfEvents.foreach {
      case (f, howManyRuns) =>
        if (howManyRuns > 0) {
          val allTimesButSlowest = getAllTimesButSlowest(myParser, f.name, howManyRuns)
          val totalRunTime = allTimesButSlowest.sum
          val avgTime = totalRunTime.toDouble / allTimesButSlowest.size.toDouble
          val avgTimePerEvent = avgTime / f.eventsItContains

          info(
            s"""
               | Parsing ${allTimesButSlowest.size} times of resource file '${f.name}' (${f.eventsItContains} events),
               |  took ${avgTime} ms. on average (${avgTimePerEvent} ms. per event)
             """.stripMargin)
          // info(s"Parsing of resource file '${f.name}' (containing ${f.eventsItContains} events), ${allTimesButSlowest.size} times took ${totalRunTime} ms. (${avgTime} ms. on average)")
          assert(avgTimePerEvent <= maxTimePerEventInMs)
        }
    }
  }

  val resourceFileName50 = "sample.50.xml"
  s"Result of parsing a CanPipe XML" should "contain events with id's" in {
    val myParser = new CanPipeParser()
    val setOfEvents = myParser.parseFromResources(resourceFileName50)

    assert(setOfEvents.forall(_.get("/root/Event/@id").isDefined))

  }

  s"Parsing a CanPipe XML" should "filter BOTs, if told so" in {
    val filterRules: Set[FilterRule] = Set(RejectRule(name = "/root/Event/user/browser", values = Set("BOT")))
    val parserFilteringBOTs = new CanPipeParser(filterRules)
    val setOfEvents = parserFilteringBOTs.parseFromResources(resourceFileName50)

    withClue("No events left after filtering") {
      assert(setOfEvents.size > 0)
    }
    withClue("At least one event has browser = 'BOT'") {
      assert(setOfEvents.forall { mapOfResult =>
        mapOfResult.getOrElse("/root/Event/user/browser", "no-robot") != "BOT"
      })
    }
    withClue("No events has browser field") {
      assert(setOfEvents.exists { mapOfResult =>
        mapOfResult.getOrElse("/root/Event/user/browser", "no-robot") != "no-robot"
      })
    }
  }

  /*
  TODO
  it should "contains only headings for events parsed" in {
    val myParser = new CanPipeParser()
    val (setOfEvents, headings) = myParser.parseFromResources(resourceFileName50)

    withClue("NO headings!") {
      assert(headings.size > 0)
    }
    val eventIdsInHeadings = headings.map(_.event_id).toSet
    val allEventIds = setOfEvents.map(_.get("/root/Event/@id").get.head) // if something goes wrong this line will blow up. I am OK with that
    val idsInHeadingsOnly = (eventIdsInHeadings -- allEventIds)
    withClue(s"ids in headings, not in results: ${idsInHeadingsOnly.mkString(start = "{", end = "}", sep = ",")}") {
      assert(idsInHeadingsOnly.isEmpty)
    }
  }
  */

}

// end of file
