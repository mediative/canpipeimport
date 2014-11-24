package canpipe.parser

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import canpipe.parser.Base.CanPipeParser

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

}

// end of file
