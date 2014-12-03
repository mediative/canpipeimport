package util.types.reader

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.types.reader.Base.{ BooleanReader, DoubleReader, IntReader, LongReader }

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "A reader for a Long" should "properly parse Strings with Long representations" in {
    List(2L, 0L, -1L, Long.MaxValue, Long.MinValue).foreach { aLong =>
      val optLong = LongReader.read(s"${aLong}")
      assert(optLong.isDefined)
      assert(optLong.get == aLong)
    }
  }

  it should "not read random Strings" in {
    List("car", "notanumber").foreach { aString =>
      val optLong = LongReader.read(s"${aString}")
      assert(!optLong.isDefined)
    }
  }

  "A reader for an Int" should "properly parse Strings with Int representations" in {
    List(2, 0, -1, Int.MaxValue, Int.MinValue).foreach { anInt =>
      val optLong = IntReader.read(s"${anInt}")
      assert(optLong.isDefined)
      assert(optLong.get == anInt)
    }
  }

  it should "not read random Strings" in {
    List("car", "notanumber").foreach { aString =>
      val optLong = IntReader.read(s"${aString}")
      assert(!optLong.isDefined)
    }
  }

  "A reader for a Double" should "properly parse Strings with Double representations" in {
    List(2.1, 0.0, -1.0, Double.MaxValue, Double.MinValue).foreach { aDouble =>
      val optLong = DoubleReader.read(s"${aDouble}")
      assert(optLong.isDefined)
      assert(optLong.get == aDouble)
    }
  }

  it should "not read random Strings" in {
    List("car", "notanumber").foreach { aString =>
      val optLong = DoubleReader.read(s"${aString}")
      assert(!optLong.isDefined)
    }
  }

  "A reader for a Boolean" should "properly parse Strings with Boolean representations = 'True'" in {
    List("true", "tRue", "success").foreach { aBooleanAsStringTrue =>
      val optLong = BooleanReader.read(aBooleanAsStringTrue)
      assert(optLong.isDefined)
      assert(optLong.get == true)
    }
  }

  it should "properly parse Strings with Boolean representations = 'False'" in {
    List("false", "False", "failed").foreach { aBooleanAsStringFalse =>
      val optLong = BooleanReader.read(aBooleanAsStringFalse)
      assert(optLong.isDefined)
      assert(optLong.get == false)
    }
  }

  it should "not read random Strings" in {
    List("car", "notanumber").foreach { aString =>
      val optLong = BooleanReader.read(s"${aString}")
      assert(!optLong.isDefined)
    }
  }

}
