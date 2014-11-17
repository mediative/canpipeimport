package util

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.Base.CanPipe
import util.Base.CanPipe._
import util.Base.CanPipe.CanPipeParser

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "XPath" should "work for 'fromReverseNameList'" in {
    assert(XPath.fromReverseNameList(List("salut", "luis")).endsWith("salut"))
  }

  it should "work for 'toReverseNameList'" in {
    val anXPathWithNoExtra = s"salut${XPath.SEP}luis"
    assert(XPath.toReverseNameList(anXPathWithNoExtra).head == "luis")
    assert(XPath.toReverseNameList(s"${XPath.SEP}${anXPathWithNoExtra}").head == "luis")
  }

  it should "work for 'add'" in {
    val s = "/root"
    val aP = XPath(s)
    val s2 = "something"
    val xPathAsString = XPath.add(aP, s2).asString
    assert(xPathAsString.endsWith(s2))
  }

  it should "work for 'removeLast'" in {
    val s = "/root"
    val aP = XPath(s)
    val s2 = "something"
    val xPathResult = XPath.removeLast(XPath.add(aP, s2))
    assert(xPathResult.asString == aP.asString)
  }

  it should "work for default 'fromRoot'" in {
    withClue("it doesn't end with 'root'") { assert(XPath.fromRoot().asString.endsWith("root")) }
    withClue(s"it doesn't start with '${XPath.SEP} ") { assert(XPath.fromRoot().asString.startsWith(XPath.SEP)) }
  }

  it should "work for generic 'fromRoot'" in {
    val stringForRoot = "papa"
    withClue(s"it doesn't end with specified string (now: '${stringForRoot}')") { assert(XPath.fromRoot(stringForRoot).asString.endsWith(stringForRoot)) }
    withClue(s"it doesn't start with '${XPath.SEP}'") { assert(XPath.fromRoot().asString.startsWith(XPath.SEP)) }
  }

  def timeInMs[R](block: => R): (Long, R) = {
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

  case class fileFromResources(name: String, eventsItContains: Long)
  val resourceFileNamesAndNumberOfEvents: List[(fileFromResources, Int)] = List(
    (fileFromResources("sample.5000.xml", 5000), -5), (fileFromResources("sample.50.xml", 50), 10))

  s"Parsing of 'n' events from a CanPipe XML" should s"take less than '1.5 * n' ms. (on average) " in { // TODO: too slow!!!!!!
    val myParser = new CanPipeParser()
    resourceFileNamesAndNumberOfEvents.foreach {
      case (f, howManyRuns) =>
        if (howManyRuns > 0) {
          val allTimesButSlowest = getAllTimesButSlowest(myParser, f.name, howManyRuns)
          val totalRunTime = allTimesButSlowest.sum
          val avgTime = totalRunTime / howManyRuns

          info(s"Parsing of resource file '${f.name}' (containing ${f.eventsItContains} events), ${howManyRuns} times took ${totalRunTime} ms. (${avgTime} ms. on average)")
          assert(avgTime <= f.eventsItContains * 1.5)
        }
    }
  }

  val resourceFileName50 = "sample.50.xml"
  s"Result of parsing a CanPipe XML" should "contain events with id's" in {
    val myParser = new CanPipeParser()
    val (setOfEvents, _) = myParser.parseFromResources(resourceFileName50)

    assert(setOfEvents.forall(_.get("/root/Event/@id").isDefined))

  }

  s"Parsing a CanPipe XML" should "filter BOTs, if told so" in {
    val filterRules: Set[FilterRule] = Set(RejectRule(name = "/root/Event/user/browser", values = Set("BOT")))
    val parserFilteringBOTs = new CanPipeParser(filterRules)
    val (setOfEvents, _) = parserFilteringBOTs.parseFromResources(resourceFileName50)

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

  it should "contains only headings for events parsed" in {
    val parserFilteringBOTs = new CanPipeParser()
    val (setOfEvents, headings) = parserFilteringBOTs.parseFromResources(resourceFileName50)

    withClue("NO headings!") { assert(headings.size > 0) }
    val eventIdsInHeadings = headings.map(_.event_id).toSet
    val allEventIds = setOfEvents.map(_.get("/root/Event/@id").get.head) // if something goes wrong this line will blow up. I am OK with that
    val idsInHeadingsOnly = (eventIdsInHeadings -- allEventIds)
    withClue(s"ids in headings, not in results: ${idsInHeadingsOnly.mkString(start = "{", end = "}", sep = ",")}") {
      assert(idsInHeadingsOnly.isEmpty)
    }
  }

  /*
  it should "filter BOTs, if told so" in {
    import CanPipe._
    import CanPipe.FieldImportance._


    // define filtering rules for this XML:
    val filterRules: Set[FilterRule] = Set(RejectRule(name = "/root/Event/user/browser", values = Set("BOT")))
    // parser:
    val parserFilteringBOTs = new CanPipeParser(filterRules)
    val (timeItTook, (setOfImpressions, headings)) = timeInMs(parserFilteringBOTs.parseFromResources(resourceFileName50))

    val listOfMissing: List[(FieldImportance.Value, Map[String, Long])] =
      List(Must, Should).map { importanceLevel =>
        (importanceLevel,
          setOfImpressions.foldLeft(Map.empty[String, Long]) {
            case (mapOfMissing, mapOfResult) =>
              (fieldsDef.filter { case (_, importance) => importance == importanceLevel }.keySet -- mapOfResult.keySet).foldLeft(mapOfMissing) { (theMap, aFieldName) =>
                theMap + (aFieldName -> (theMap.getOrElse(aFieldName, 0L) + 1))
              }
          })
      }
    // report of SHOULD/MUST fields that are missing:
    println(s"**************  Report of fields that are missing, on the sampling of ${setOfImpressions.size} impressions **************")
    listOfMissing.foreach {
      case (importanceLevel, report) =>
        println(s"**************  Report of ${importanceLevel} fields that are missing  **************")
        report.foreach {
          case (aFieldName, howManyTimesMissing) =>
            println(s"'${aFieldName}', missed ${howManyTimesMissing} times")
        }
    }

    println(s"Table [headings] has ${headings.size} entries")

    /*
    logger.info(s"'${setOfImpressions.size} IMPRESSION events parsed  ==> ")
    setOfImpressions.foreach { mapOfResult =>
      // display all:
      logger.info("**************  Report of ALL fields  **************")
      mapOfResult.foreach {
        case (fieldName, listOfValues) =>
          logger.info(s"\t 'field = ${fieldName}', value = ${listOfValues.mkString(start = "{", sep = ",", end = "}")}")
      }
      // val eD = eventDetail(mapOfResult)
      // logger.info(s"${eD.toString}")
    }
    */

  }
  */

}
