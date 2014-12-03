package util.errorhandler

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.errorhandler.Base.{ BooleanHandler, DoubleHandler, IntHandler, LongHandler }

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "An error handler for a Long" should "properly recognize success conditions" in {
    List(2L, 0L, -1L, Long.MaxValue, Long.MinValue).foreach { aLong =>
      LongHandler(Some(aLong)) match {
        case Left(_) => assert(false)
        case Right(_) => assert(true)
      }
    }
  }

  it should "properly recognize failure conditions" in {
    LongHandler(None) match {
      case Left(_) => assert(true)
      case Right(_) => assert(false)
    }
  }

  "An error handler for an Int" should "properly recognize success conditions" in {
    List(2, 0, -1, Int.MaxValue, Int.MinValue).foreach { anInt =>
      IntHandler(Some(anInt)) match {
        case Left(_) => assert(false)
        case Right(_) => assert(true)
      }
    }
  }

  it should "properly recognize failure conditions" in {
    IntHandler(None) match {
      case Left(_) => assert(true)
      case Right(_) => assert(false)
    }
  }

  "An error handler for a Double" should "properly recognize success conditions" in {
    List(2.1, 0.1, -1.0, Double.MaxValue, Double.MinValue).foreach { aDouble =>
      DoubleHandler(Some(aDouble)) match {
        case Left(_) => assert(false)
        case Right(_) => assert(true)
      }
    }
  }

  it should "properly recognize failure conditions" in {
    DoubleHandler(None) match {
      case Left(_) => assert(true)
      case Right(_) => assert(false)
    }
  }

  "An error handler for a Boolean" should "properly recognize success conditions" in {
    List(true, false).foreach { aBoolean =>
      BooleanHandler(Some(aBoolean)) match {
        case Left(_) => assert(false)
        case Right(_) => assert(true)
      }
    }
  }

  it should "properly recognize failure conditions" in {
    BooleanHandler(None) match {
      case Left(_) => assert(true)
      case Right(_) => assert(false)
    }
  }

}
