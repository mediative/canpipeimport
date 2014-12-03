package util

import org.scalatest.{ BeforeAndAfter, FlatSpec }

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "Random String generated" should "be of the right length" in {
    val length = 10
    assert(Base.String.generateRandom(length).length == length)
  }

  it should "change on every invocation" in {
    val length = 13
    val listOf10RandomStrings = (1 to 10).map { _ => Base.String.generateRandom(length) }
    assert(listOf10RandomStrings.forall { anElem => listOf10RandomStrings.filter(_ == anElem).length == 1 })
  }

} // end of file
