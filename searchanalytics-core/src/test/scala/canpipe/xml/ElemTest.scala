package canpipe.xml

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.xml.{ Field => XMLField }
import util.xml.Base._

class ElemTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "Converting an XML into Canpipe's XML" should "work if all important fields are there" in {
    val eventsXML =
      <root>
        <Event id="3a" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3b" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>
    val canpipeElemOpt = CanpipeXMLElem(xml.XML.loadString(eventsXML.toString()))
    assert(canpipeElemOpt.isDefined)
    val scalaXMLElem = canpipeElemOpt.get.value
    // make sure the ids are properly read
    assert((scalaXMLElem \ XMLField(s"Event/@id").asList).toSet == Set("3a", "3b"))
  }

  it should "fail if NO event ids are present" in {
    val eventsXML =
      <root>
        <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>
    assert(!CanpipeXMLElem(xml.XML.loadString(eventsXML.toString())).isDefined)
  }

  it should "fail if NO timestamps are present" in {
    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>
    assert(!CanpipeXMLElem(xml.XML.loadString(eventsXML.toString())).isDefined)
  }

}
