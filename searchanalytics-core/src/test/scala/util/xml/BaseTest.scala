package util.xml

import org.scalacheck._
import Base._

object Data {

  object CanpipeXML {
    val anXML =
      <root>
        <Event id="3a" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression">lala3a</Event>
        <Event id="3b" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression">lala3b</Event>
      </root>
    val eventAttribs = Set("@id", "@timestamp", "@site", "@siteLanguage", "@eventType", "@isMap", "@typeOfLog")
  }

  object RandomXML {
    val anotherXML =
      <root>
        <a>
          <b>luis</b>
          <c>
            <d>d</d>
          </c>
        </a>
      </root>
    val fieldsWithValues: Map[List[String], String] = Map(List("a", "b") -> "luis", List("a", "c", "d") -> "d")
  }

  val validLines = Set(
    "<a></a>",
    "    <a></a>     ",
    "<a id=1 anotherfield=2>something-something here</a>",
    "<a><b></b></a>",
    "   <a><b></b></a>",
    "<a><b></b></a>      ")
  val invalidLines = Set(
    "<a></b>",
    "<a><b></b>",
    "lalalala",
    "",
    "something<a></a>",
    "<a></a>something",
    "<a>",
    "</a>")
}

object BaseTest extends Properties("XML") {
  import Prop.forAll
  import Data.CanpipeXML.anXML
  import Data.RandomXML.anotherXML
  import Data._

  val existingFields = Gen.oneOf(Data.CanpipeXML.eventAttribs.map(List("Event", _)).toSeq)
  val nonExistingFields = Gen.oneOf(List("Event", "@id2"), List("Event", "@timestamp1"), List.empty)
  val existingFieldsInRandom = Gen.oneOf(Data.RandomXML.fieldsWithValues.keys.toSeq)
  val validLinesGen = Gen.oneOf(validLines.toList)
  val invalidLinesGen = Gen.oneOf(invalidLines.toList)

  property("\\.findsExistingFieldsInCanpipe") = forAll(existingFields) { aField: List[String] =>
    (anXML \ aField).length == 2
  }

  property("\\.doesNotFindStupidFieldsInCanpipe") = forAll(nonExistingFields) { aField: List[String] =>
    (anXML \ aField).isEmpty
  }

  property("\\.findsExistingFieldsInRandom") = forAll(existingFieldsInRandom) { aField: List[String] =>
    val r = (anotherXML \ aField)
    (r.length == 1) && (r.head == Data.RandomXML.fieldsWithValues.getOrElse(aField, ""))
  }

  property("isValidEntry.recognizesProperXMLs") = forAll(validLinesGen) { isValidEntry(_) }
  property("isValidEntry.recognizesImproperXMLs") = forAll(invalidLinesGen) { !isValidEntry(_) }

}

