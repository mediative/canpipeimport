package canpipe

import canpipe.EventDetail.{ BasicInfo, AnalyticsTimestampId, Language }
import org.scalacheck._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{ Matchers, BeforeAndAfter, FreeSpec }

class EventDetailTest extends FreeSpec with BeforeAndAfter with Matchers with GeneratorDrivenPropertyChecks {

  before {

  }

  after {

  }

  "Language matcher should " - {
    "recognize English hints when" - {
      "appearing as a long word" in {
        val langOpt = Language.apply("English")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.EN)
      }
      "appearing as a lowercase word" in {
        val langOpt = Language.apply("english")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.EN)
      }
      "appearing as an UPPERCASE word" in {
        val langOpt = Language.apply("ENGLISH")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.EN)
      }
      "appearing as a 2-letter abbreviation" in {
        val langOpt = Language.apply("EN")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.EN)
      }
    }
    "recognize French hints when" - {
      "appearing as a long word" in {
        val langOpt = Language.apply("French")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.FR)
      }
      "appearing as a lowercase word" in {
        val langOpt = Language.apply("french")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.FR)
      }
      "appearing as an UPPERCASE word" in {
        val langOpt = Language.apply("FRENCH")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.FR)
      }
      "appearing as a 2-letter abbreviation" in {
        val langOpt = Language.apply("FR")
        assert(langOpt.isDefined)
        assert(langOpt.get == Language.FR)
      }
    }
    "discard empty hints" in {
      assert(!Language.apply("").isDefined)
    }
    "discard any 2-letter string other than 'EN' and 'FR'" in {
      val twoLettersStringGen: Gen[String] = Gen.listOfN(2, Gen.alphaChar).map(_.mkString)
      forAll(twoLettersStringGen) { (s: String) =>
        val upperS = s.trim.toUpperCase
        whenever((upperS != "FR") && (upperS != "EN")) { Language.apply(s).isDefined should be(false) }
      }
    }
    "discard any string other than 'English' and 'French'" in {
      forAll(Gen.alphaStr) { (s: String) =>
        val upperS = s.trim.toUpperCase
        whenever(!upperS.startsWith("FR") && !upperS.startsWith("EN")) { Language.apply(s).isDefined should be(false) }
      }
    }
  }

  "Timestamp generator should" - {
    "recognize a fully-fledged date when" - {
      "specified as string" in {
        assert(AnalyticsTimestampId("2015-11-06T11:01:02.987-55:12").isDefined)
      }
      "specified as int" in {
        assert(AnalyticsTimestampId(20151106).isDefined)
      }
    }
    "discard random values when" - {
      "they are Strings" in {
        forAll(Gen.alphaStr) { (s: String) =>
          AnalyticsTimestampId(s).isDefined should be(false)
        }
      }
      "they are Ints" in {
        forAll(for (n <- Gen.choose(-1000, 1000)) yield n) { (n: Int) =>
          AnalyticsTimestampId(n).isDefined should be(false)
        }
      }
    }
  }

  "Event.BasicInfo should " - {
    case class GoodBadGenerators[T](good: Gen[T], bad: Gen[T])
    val timestampGenerators =
      GoodBadGenerators[String](good = Gen.oneOf("2015-11-06T11:01:02.987-55:12", "2015-11-06T11:01:02.987-55:13"), bad = Gen.alphaStr)
    val langGenerators =
      GoodBadGenerators[String](good = Gen.oneOf("EN", "FR"), bad = Gen.alphaStr.filter(s => !s.toUpperCase.startsWith("EN") && !s.toUpperCase.startsWith("FR")))
    val durationGenerators =
      GoodBadGenerators[Long](good = Gen.choose(0, 10000), bad = Gen.choose(-10000, -1))
    def withTheseGenerators(
      timestampGen: Gen[String] = timestampGenerators.good,
      langGen: Gen[String] = langGenerators.good,
      durationGen: Gen[Long] = durationGenerators.good,
      expectedResult: Boolean) = {
      forAll(timestampGen, Gen.alphaStr, Gen.alphaStr, langGen, Gen.alphaStr, Gen.alphaStr) {
        (timestamp: String, anId: String, aSite: String, lang: String, user: String, apiK: String) =>
          forAll(Gen.alphaStr, durationGen, Gen.oneOf(false, true), Gen.alphaStr, Gen.alphaStr, Gen.alphaStr) {
            (session: String, aDuration: Long, cached: Boolean, ref: String, pName: String, uri: String) =>
              BasicInfo(
                id = anId, timestamp = timestamp, site = aSite,
                siteLanguage = lang, userId = user, apiKey = apiK,
                userSessionId = session, transactionDuration = aDuration,
                isResultCached = cached, referrer = ref, pageName = pName,
                requestUri = uri).isDefined should be(expectedResult)
          }
      }

    }
    "not be constructed when" - {
      "all fields are correct but" - {
        "timestamp" in {
          withTheseGenerators(timestampGen = timestampGenerators.bad, expectedResult = false)
        }
        "language" in {
          withTheseGenerators(langGen = langGenerators.bad, expectedResult = false)
        }
        "transaction duration" in {
          withTheseGenerators(durationGen = durationGenerators.bad, expectedResult = false)
        }
      }
    }
    "be constructed when all fields are valid" in {
      withTheseGenerators(expectedResult = true)
    }
  }

}
