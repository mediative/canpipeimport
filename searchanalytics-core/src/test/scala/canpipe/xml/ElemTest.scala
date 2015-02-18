package canpipe.xml

import org.scalatest.{ BeforeAndAfterAll, FreeSpec, BeforeAndAfter, FlatSpec }
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.xml.{ Field => XMLField }
import util.xml.Base._

class ElemTest extends FreeSpec with BeforeAndAfterAll {

  override def beforeAll() {
  }

  override def afterAll() {
  }

  "Converting an XML into Canpipe's XML" - {
    "should fail when" - {
      "not all events have ids" in {
        val eventsXML =
          <root>
            <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
          </root>
        assert(!CanpipeXMLElem(xml.XML.loadString(eventsXML.toString())).isDefined)
      }
      "not all events have timestamps" in {
        val eventsXML =
          <root>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
          </root>
        assert(!CanpipeXMLElem(xml.XML.loadString(eventsXML.toString())).isDefined)
      }
    }

    "when reading a proper XML should" - {
      val eventsXML =
        <root>
          <Event id="3a" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
          <Event id="3b" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        </root>
      val canpipeElemOpt = CanpipeXMLElem(xml.XML.loadString(eventsXML.toString()))
      withClue("Start testing fail: can not build Canpipe's XML out of example") { assert(canpipeElemOpt.isDefined) }
      val canpipeElem = canpipeElemOpt.get

      "report the proper number of events" in {
        assert(canpipeElem.numChilds == 2)
      }

      "return correct events' ids" in {
        val scalaXMLElem = canpipeElemOpt.get.value
        // make sure the ids are properly read
        assert((scalaXMLElem \ XMLField(s"Event/@id").asList).toSet == Set("3a", "3b"))
      }
    }
  }

}
