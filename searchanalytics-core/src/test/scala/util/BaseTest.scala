package util

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.Base._

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

  val stringsThatAreBooleanAndTrue = List("true", "t", "T")
  val stringsThatAreBooleanAndFalse = List("false", "False", "NO", "f", "F")

  "Converting a String to Boolean" should s"return 'true' when String is one of ${stringsThatAreBooleanAndTrue.mkString(start = "{", end = "}", sep = ",")} " in {
    stringsThatAreBooleanAndTrue foreach { aString =>
      val bOpt = String.toBooleanOpt(aString)
      assert(bOpt.isDefined)
      assert(bOpt.get)
    }
  }

  it should s"return 'false' when String is one of ${stringsThatAreBooleanAndFalse.mkString(start = "{", end = "}", sep = ",")} " in {
    stringsThatAreBooleanAndFalse foreach { aString =>
      val bOpt = String.toBooleanOpt(aString)
      assert(bOpt.isDefined)
      assert(!bOpt.get)
    }
  }

  "Filling a list up to a certain size" should "work when list is already full" in {
    val l = List(1, 2, 3)
    assert(fillToMinimumSize[Int](aList = l, n = 1, default = -1) == l)
  }

  it should "work when size is negative" in {
    val l = List(1, 2, 3)
    assert(fillToMinimumSize[Int](aList = l, n = -1, default = -1) == l)
  }

  it should "work in a general setting" in {
    val l = List(1, 2, 3)
    val theSize = 5
    assert(fillToMinimumSize[Int](aList = l, n = theSize, default = -1).length == theSize)
  }
} // end of file
