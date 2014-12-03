package util.xml

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import Base._

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "Lookup in XMLs" should "work for tags" in {
    val anXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression">lala</Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression">lala</Event>
      </root>
    assert((anXML \ List("Event", "@id")).length == 2)
    assert((anXML \ List("Event", "@timestamp")).length == 2)
    assert((anXML \ List("Event", "@timestamp1")).isEmpty)
  }

  it should "work for internal paths" in {
    val anXML =
      <root>
        <a>
          <b>luis</b>
          <c>
            <d>d</d>
          </c>
        </a>
      </root>
    val l = (anXML \ List("a", "b"))
    assert(l.length == 1)
    assert(l.head == "luis")
  }

}
