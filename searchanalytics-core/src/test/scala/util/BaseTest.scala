package util

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import Base.XML._

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

} // end of file
