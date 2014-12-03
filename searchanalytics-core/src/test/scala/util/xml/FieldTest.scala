package util.xml

import org.scalatest.{ BeforeAndAfter, FlatSpec }

class FieldTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  val pathAsList = List("root", "event", "papa")
  s"Value of ${Field.fromList(pathAsList).asString} as list" should s"be ${pathAsList.toString()}" in {
    assert(Field.fromList(pathAsList).asList == pathAsList)
  }

  "XPath" should "work for 'fromReverseNameList'" in {
    assert(Field.fromReverseNameList(List("salut", "luis")).endsWith("salut"))
  }

  it should "work for 'toReverseNameList'" in {
    val anXPathWithNoExtra = s"salut${Field.SEP}luis"
    assert(Field.toReverseNameList(anXPathWithNoExtra).head == "luis")
    assert(Field.toReverseNameList(s"${Field.SEP}${anXPathWithNoExtra}").head == "luis")
  }

  it should "work for 'add'" in {
    val s = "/root"
    val aP = Field(s)
    val s2 = "something"
    val xPathAsString = Field.add(aP, s2).asString
    assert(xPathAsString.endsWith(s2))
  }

  it should "work for 'removeLast'" in {
    val s = "/root"
    val aP = Field(s)
    val s2 = "something"
    val xPathResult = Field.removeLast(Field.add(aP, s2))
    assert(xPathResult.asString == aP.asString)
  }

  it should "work for default 'fromRoot'" in {
    withClue("it doesn't end with 'root'") { assert(Field.fromRoot().asString.endsWith("root")) }
    withClue(s"it doesn't start with '${Field.SEP} ") { assert(Field.fromRoot().asString.startsWith(Field.SEP)) }
  }

  it should "work for generic 'fromRoot'" in {
    val stringForRoot = "papa"
    withClue(s"it doesn't end with specified string (now: '${stringForRoot}')") { assert(Field.fromRoot(stringForRoot).asString.endsWith(stringForRoot)) }
    withClue(s"it doesn't start with '${Field.SEP}'") { assert(Field.fromRoot().asString.startsWith(Field.SEP)) }
  }

}
