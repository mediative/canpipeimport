package util

import org.scalatest.{ BeforeAndAfter, FlatSpec }

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  private def waitForThisManySeconds(numSeconds: Int): Unit = { Thread.sleep(numSeconds * 1000) }

  "Random String generated" should "be of the right length" in {
    val length = 10
    assert(Base.String.generateRandom(length).length == length)
  }

  it should "change on every invocation" in {
    val length = 13
    val listOf10RandomStrings = (1 to 10).map { _ => Base.String.generateRandom(length) }
    assert(listOf10RandomStrings.forall { anElem => listOf10RandomStrings.filter(_ == anElem).length == 1 })
  }

  "Timing functions" should "report the right amount in milliseconds (at 0.5% precision)" in {
    val numSeconds = 3
    val perfectTimeInMs = numSeconds * 1000
    val actualTimeInMs = Base.timeInMs(waitForThisManySeconds(numSeconds)).run._1
    val tol = perfectTimeInMs * 0.005
    withClue(s"tol = ${tol}, actual time (in ms.) = ${actualTimeInMs}") { assert((actualTimeInMs >= perfectTimeInMs - tol) && (actualTimeInMs <= perfectTimeInMs + tol)) }
  }

  it should "report the right amount in nanoseconds (at 0.5% precision)" in {
    val numSeconds = 4
    val perfectTimeInNanoSecs = numSeconds * 1.0E9
    val actualTimeInNanoSecs = Base.timeInNanoSecs(waitForThisManySeconds(numSeconds)).run._1
    val tol = perfectTimeInNanoSecs * 0.005
    withClue(s"tol = ${tol}, actual time (in ms.) = ${actualTimeInNanoSecs}") { assert((actualTimeInNanoSecs >= perfectTimeInNanoSecs - tol) && (actualTimeInNanoSecs <= perfectTimeInNanoSecs + tol)) }
  }

} // end of file
