package canpipe

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{ Matchers, BeforeAndAfterAll, FreeSpec }
import canpipe.xml.{ Elem => CanpipeElem, XMLFields }
import util.xml.{ Field => XMLField }

import scales.utils._
import ScalesUtils._ // import implicits
import scales.xml._
import ScalesXml._ // import implicits

class TablesTest extends FreeSpec with BeforeAndAfterAll with Matchers with GeneratorDrivenPropertyChecks {

  override def beforeAll() {
  }

  override def afterAll() {
  }

  val eventsXML =
    <root>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      <Event id="22fcfb29-b1aa-42f5-89aa-a93b4e8b7884" timestamp="2015-01-13T20:51:42.695-05:00" site="api" siteLanguage="EN" eventType="click" isMap="false" userId="api-ypg-searchapp-android-html5" typeOfLog="click"><apiKey></apiKey><sessionId>577754b58ef174e82</sessionId><transactionDuration>16</transactionDuration><cachingUsed>false</cachingUsed><applicationId>7</applicationId><referrer>http://mobile.yp.ca/bus/Alberta/Lethbridge/Royal-Satellite-Sales-Service/5194731.html?product=L2</referrer><user><id>251790082</id><ip>23.22.220.184</ip><deviceId>MOBILE</deviceId><userAgent>AndroidNativeYP/3.4.1 (Linux; U; Android 2.3.5; en-ca; SGH-T989D Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1</userAgent><browser>MOBILE_SAFARI</browser><browserVersion>4.0</browserVersion><os>ANDROID2</os></user><search><searchId>577754b58ef174e82_R2Fz_Q3VycmVudCBsb2NhdGlvbg_1</searchId><searchBoxUsed>false</searchBoxUsed><disambiguationPopup>N</disambiguationPopup><failedOrSuccess>Success</failedOrSuccess><calledBing>N</calledBing><matchedGeo/><allListingsTypesMainLists></allListingsTypesMainLists><relatedListingsReturned>false</relatedListingsReturned><allHeadings><heading><name>01157630</name><category>B</category></heading><heading><name>01157800</name><category>B</category></heading></allHeadings><type>merchant</type><radius_1>0.0</radius_1><radius_2>0.0</radius_2><merchants id="5194731" zone="" latitude="49.662243" longitude="-112.8771"><isListingRelevant>true</isListingRelevant><entry><headings><categories>01157630,01157800</categories></headings><directories><channel1>085750</channel1></directories><listingType>L2</listingType></entry></merchants><searchAnalysis><fuzzy>false</fuzzy><geoExpanded>false</geoExpanded><businessName>false</businessName></searchAnalysis></search><requestUri>/merchant/5194731?what=Gas&amp;where=Current+location&amp;fmt=JSON&amp;ypid=7&amp;sessionid=577754b58ef174e82</requestUri><searchAnalytics><entry key="refinements" value=""/><entry key="geoPolygons" value=""/><entry key="geoProvince" value=""/><entry key="geoDirectories" value="092170"/><entry key="collatedWhat" value=""/><entry key="relevantHeadings" value="00617200,01186800,00924800,00618075,00901275"/><entry key="geoCentroid" value="43.9328429,-78.6967274"/><entry key="geoType" value="X"/><entry key="collatedWhere" value=""/><entry key="geoName" value="Current location"/></searchAnalytics></Event>
    </root>

  "Getting a field from a 'Canpipe' XML should" - {
    val xmlFieldGen: Gen[XMLField] = Gen.oneOf(XMLFields.apiKey, XMLFields.categoryId, XMLFields.eventSite,
      XMLFields.eventSiteLanguage, XMLFields.eventType, XMLFields.userId, XMLFields.apiKey, XMLFields.userSessionId,
      XMLFields.transactionDuration, XMLFields.isResultCached, XMLFields.eventReferrer, XMLFields.pageName,
      XMLFields.requestUri,
      XMLFields.userIP, XMLFields.userAgent, XMLFields.userIsRobot, XMLFields.userLocation, XMLFields.userBrowser,
      XMLFields.searchId, XMLFields.searchWhat, XMLFields.searchWhere, XMLFields.searchResultCount, XMLFields.searchWhatResolved,
      XMLFields.searchIsDisambiguation, XMLFields.searchIsSuggestion, XMLFields.searchFailedOrSuccess,
      XMLFields.searchHasRHSListings, XMLFields.searchHasNonAdRollupListings, XMLFields.searchIsCalledBing,
      XMLFields.searchGeoOrDir, XMLFields.categoryId, XMLFields.tierId, XMLFields.tierCount, XMLFields.searchGeoName,
      XMLFields.searchGeoType, XMLFields.searchGeoPolygonIds, XMLFields.tierUdacCountList, XMLFields.directoriesReturned,
      XMLFields.headingId, XMLFields.headingRelevance, XMLFields.searchType, XMLFields.searchResultPage,
      XMLFields.searchResultPerPage, XMLFields.searchLatitude, XMLFields.searchLongitude,
      XMLFields.merchantId, XMLFields.merchantZone, XMLFields.merchantLatitude, XMLFields.merchantLongitude,
      XMLFields.merchantDistance, XMLFields.merchantDisplayPosition, XMLFields.merchantIsNonAdRollup,
      XMLFields.merchantRank, XMLFields.merchantIsRelevantListing, XMLFields.merchantIsRelevantHeading,
      XMLFields.merchantHeadingIdList, XMLFields.merchantChannel1List, XMLFields.merchantChannel2List,
      XMLFields.productType, XMLFields.productLanguage, XMLFields.productUdac, XMLFields.merchantListingType,
      XMLFields.searchAnalysisIsfuzzy, XMLFields.searchAnalysisIsGeoExpanded, XMLFields.searchAnalysisIsBusinessName,
      XMLFields.key, XMLFields.value)

    "return 'empty' when" - {
      "XML is empty" in {
        forAll(xmlFieldGen) { (anXMLField: XMLField) =>
          assert(Tables.getOrEmpty(CanpipeElem(<root></root>).get, anXMLField).isEmpty)
        }
      }

      "baby lala" in {

      }

    }

    "return the same count as events when" - {
      val canpipeXMLOpt = CanpipeElem(eventsXML)
      withClue("Start testing fail: can not build Canpipe's XML out of example") { assert(canpipeXMLOpt.isDefined) }
      val canpipeXML = canpipeXMLOpt.get
      val totalElems = canpipeXML.value.child.count(!_.isAtom)
      info(s"Total elems = ${totalElems}")

      "field is mandatory" in {
        val ss = Tables.getOrEmpty(canpipeXML, XMLFields.eventId)
        info(ss)
        assert(Tables.getOrEmpty(canpipeXML, XMLFields.eventId).length == totalElems)
      }
    }

  }

  "'Tables'" - {

    "should yield an empty result when parsing" - {

      "an empty XML " in {
        CanpipeElem(<root></root>).map { aCanpipeElem =>
          val tables = Tables(aCanpipeElem)
          val eventsOpt = tables.eventOpt
          withClue("Events defined") { assert(!eventsOpt.isDefined) }
        }.getOrElse { withClue("Canpipe Elem not built") { assert(false) } }
      }
    }

    /*
    "should report the proper number of events when" - {

      "all events have all fields" in {

        // third-event was hand-picked from logs
        val eventsXML =
          <root>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
            <Event id="22fcfb29-b1aa-42f5-89aa-a93b4e8b7884" timestamp="2015-01-13T20:51:42.695-05:00" site="api" siteLanguage="EN" eventType="click" isMap="false" userId="api-ypg-searchapp-android-html5" typeOfLog="click"><apiKey></apiKey><sessionId>577754b58ef174e82</sessionId><transactionDuration>16</transactionDuration><cachingUsed>false</cachingUsed><applicationId>7</applicationId><referrer>http://mobile.yp.ca/bus/Alberta/Lethbridge/Royal-Satellite-Sales-Service/5194731.html?product=L2</referrer><user><id>251790082</id><ip>23.22.220.184</ip><deviceId>MOBILE</deviceId><userAgent>AndroidNativeYP/3.4.1 (Linux; U; Android 2.3.5; en-ca; SGH-T989D Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1</userAgent><browser>MOBILE_SAFARI</browser><browserVersion>4.0</browserVersion><os>ANDROID2</os></user><search><searchId>577754b58ef174e82_R2Fz_Q3VycmVudCBsb2NhdGlvbg_1</searchId><searchBoxUsed>false</searchBoxUsed><disambiguationPopup>N</disambiguationPopup><failedOrSuccess>Success</failedOrSuccess><calledBing>N</calledBing><matchedGeo/><allListingsTypesMainLists></allListingsTypesMainLists><relatedListingsReturned>false</relatedListingsReturned><allHeadings><heading><name>01157630</name><category>B</category></heading><heading><name>01157800</name><category>B</category></heading></allHeadings><type>merchant</type><radius_1>0.0</radius_1><radius_2>0.0</radius_2><merchants id="5194731" zone="" latitude="49.662243" longitude="-112.8771"><isListingRelevant>true</isListingRelevant><entry><headings><categories>01157630,01157800</categories></headings><directories><channel1>085750</channel1></directories><listingType>L2</listingType></entry></merchants><searchAnalysis><fuzzy>false</fuzzy><geoExpanded>false</geoExpanded><businessName>false</businessName></searchAnalysis></search><requestUri>/merchant/5194731?what=Gas&amp;where=Current+location&amp;fmt=JSON&amp;ypid=7&amp;sessionid=577754b58ef174e82</requestUri><searchAnalytics><entry key="refinements" value=""/><entry key="geoPolygons" value=""/><entry key="geoProvince" value=""/><entry key="geoDirectories" value="092170"/><entry key="collatedWhat" value=""/><entry key="relevantHeadings" value="00617200,01186800,00924800,00618075,00901275"/><entry key="geoCentroid" value="43.9328429,-78.6967274"/><entry key="geoType" value="X"/><entry key="collatedWhere" value=""/><entry key="geoName" value="Current location"/></searchAnalytics></Event>
          </root>

        CanpipeElem(eventsXML).map { aCanpipeElem =>
          val tables = Tables(aCanpipeElem)
          val eventsOpt = tables.eventOpt
          withClue("Events undefined") { assert(eventsOpt.isDefined) }
          val events = eventsOpt.get
          events.basicInfo
        }.getOrElse { withClue("Canpipe Elem not built") { assert(false) } }


        val rdd = myParser.parse(new XMLPiecePerLine(sc, "root", eventsXML))
        assert(rdd.count() == 3)
      }
    }
    */

  }

}
