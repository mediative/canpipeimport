package canpipe.parser

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{ Matchers, BeforeAndAfterAll, FreeSpec }
import scales.utils._
import ScalesUtils._ // import implicits
import scales.xml._
import ScalesXml._ // import implicits
import canpipe.parser.Base._
import canpipe.xml.{ Elem => CanpipeElem }
import canpipe.event.{ raw => RawEvent }

class BaseTest extends FreeSpec with BeforeAndAfterAll with Matchers with GeneratorDrivenPropertyChecks {

  val aSimpleXML = <root><Event id="id1" timestamp="2014-10-27T22:32:24.464-04:00"></Event></root>
  val aSimpleCanpipeXML = CanpipeElem(aSimpleXML)

  override def beforeAll() {
    withClue("Internal: Simple XML is not understood by constructor") { assert(aSimpleCanpipeXML.isDefined) }
  }

  override def afterAll() {
  }

  object parserImplicits {
    implicit object VerboseableOutput extends Outputable[RawEvent.Event, String] {
      def validateFromRaw: RawEvent.Event => Option[RawEvent.Event] = re => Some(re)
      def cumulate = (c: Option[RawEvent.Event]) => (s: String) => { s + "\n" + c.toString }
      def zeroAcc = ""
    }

    implicit object CountingOutput extends Outputable[RawEvent.Event, Int] {
      def validateFromRaw: RawEvent.Event => Option[RawEvent.Event] = re => Some(re)
      def cumulate = (c: Option[RawEvent.Event]) => (i: Int) => { c.map(_ => i + 1).getOrElse(i) }
      def zeroAcc = 0
    }

    implicit object AggregatedOutput extends Outputable[RawEvent.Event, Set[RawEvent.Event]] {
      def validateFromRaw: RawEvent.Event => Option[RawEvent.Event] = re => Some(re)
      def cumulate = (c: Option[RawEvent.Event]) => (s: Set[RawEvent.Event]) => { c.map(e => s + e).getOrElse(s) }
      def zeroAcc = Set.empty
    }

  }
  "A Parser should" - {
    "print results when parsing from" - {
      import parserImplicits.VerboseableOutput

      "a Canpipe XML" in {
        info(Parser.parse(aSimpleCanpipeXML.get))
      }

      "a resource file containing a valid XML" in {
        Parser.parseFromResource("/CanPipeExample.xml").map(info(_)).getOrElse({
          withClue(s"Impossible to parse resource file") { assert(false) }
        })
      }
    }

    "count the correct number of events when parsing from" - {
      import parserImplicits.CountingOutput

      "a Canpipe XML" in {
        assert(Parser.parse(aSimpleCanpipeXML.get) === 1)
      }
      "a resource file containing a valid XML" in {
        Parser.parseFromResource("/CanPipeExample.xml").map(_ === 3).getOrElse({
          withClue(s"Impossible to parse resource file") { assert(false) }
        })
      }
    }

    "'fail' when" - {
      import parserImplicits.CountingOutput

      "asked to parse from an un-existing resource" in {
        assert(!Parser.parseFromResource("/totallynothere.xml").isDefined)
      }
    }

    "read defined fields with proper values when parsing from" - {
      import parserImplicits.AggregatedOutput

      "a simple XML" in {
        val r = Parser.parse(aSimpleCanpipeXML.get)
        withClue("Size mismatch") { assert(r.size === 1) }
        withClue("Id undefined") { assert(!r.head.id.isEmpty) }
        withClue("Timestamp undefined") { assert(!r.head.timestamp.isEmpty) }
      }
    }

    "read all elements of all events " - {
      import parserImplicits.AggregatedOutput

      "when parsing from a hand-crafted XML" in {
        Parser.parseFromResource("/CanPipeExample.xml").map { r =>
          withClue("Size mismatch") { assert(r.size === 3) }
          r.foreach { e =>
            withClue("Id undefined") { assert(!e.id.isEmpty) }
            e.search.map { s =>
              withClue("Search Id undefined") { assert(!s.searchId.isEmpty) }
              withClue(s"[Event '${e.id}'] does not have headings; count = ${s.allHeadings.headings.size}") { assert(!s.allHeadings.headings.isEmpty) }
              s.allHeadings.headings.foreach { h =>
                assert(!h.name.isEmpty)
                assert(!h.category.isEmpty)
              }
              withClue(s"[Event '${e.id}'] Search '${s.searchId}' does not have merchants") { assert(!s.merchants.isEmpty) }
            }.getOrElse(info(s"No search section for event ${e.id} (might be OK! - eg, a 'click' event)"))
          }
        }.getOrElse({
          withClue(s"Impossible to parse resource file") { assert(false) }
        })
      }

      "(generally), when parsing from a real-world XML" in {
        val (resourceFile, numEvents) = ("/sample.50.xml", 50)
        val maxBadEventsProp = 0.075 // maximum proportion of badly formed events allowed
        Parser.parseFromResource(resourceFile).map { r =>
          withClue("Size mismatch") { assert(r.size === numEvents) }
          val badlyFormedEvents =
            r.foldLeft(0) { (acc, e) =>
              withClue("Id undefined") { assert(!e.id.isEmpty) }
              val searchSectionOK =
                e.search.map { s =>
                  if (s.searchId.isEmpty) {
                    info(s"[Event '${e.id}'] Search Id undefined")
                    false
                  } else {
                    if (s.allHeadings.headings.isEmpty) {
                      info(s"[Event '${e.id}'] does not have headings")
                      false
                    } else {
                      if (!s.allHeadings.headings.forall { h => !h.name.isEmpty && !h.category.isEmpty }) {
                        info(s"[Event '${e.id}'] Search '${s.searchId}': not all headings have 'name' and 'category'")
                        false
                      } else {
                        if (s.merchants.isEmpty) {
                          info(s"[Event '${e.id}'] Search '${s.searchId}' does not have merchants")
                          false
                        } else
                          true
                      }
                    }

                  }
                }.getOrElse { info(s"No search section for event ${e.id} (might be OK! - eg, a 'click' event)"); true }
              if (!searchSectionOK) acc + 1
              else acc
            }
          info(s"${badlyFormedEvents} out of ${numEvents} events are badly formed")
          withClue(s"Too many badly formed events!") { assert(badlyFormedEvents <= numEvents * maxBadEventsProp) }
        }.getOrElse({
          withClue(s"Impossible to parse resource file '${resourceFile}' (should be here: ${getClass.getResource(resourceFile).getPath})") { assert(false) }
        })
      }
    }

  }

}
