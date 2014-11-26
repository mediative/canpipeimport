package canpipe.parser.spark

import canpipe.parser.FilterRule
import org.apache.spark.rdd.RDD
import util.Base.XML.XPath
import scala.util.control.Exception._

import scala.io.Source
import scala.xml.pull.XMLEventReader

class eventDetail(
  /* ******************************************** */
  /* Main event attributes and fields */
  val eventId: String, /* /root/Event/@id */
  val timestamp: String, /* /root/Event/@timestamp */
  val eventSite: String, /* /root/Event/@site */
  val eventSiteLanguage: String, /* /root/Event/@siteLanguage */
  // TODO: we need "/root/Event/@eventType": String, // Two values are possible "impression" which is a SERP event, or "click" which is an MP event
  val userId: String, /* /root/Event/@userId */
  val apiKey: String /* /root/Event/apiKey */ , val userSessionId: String /* /root/Event/sessionId */ ,
  val transactionDuration: Long /* /root/Event/transactionDuration */ ,
  val isResultCached: Boolean /* /root/Event/cachingUsed */ , val eventReferrer: String /* /root/Event/referrer */ ,
  val pageName: String /* /root/Event/pageName */ , val requestUri: String /* /root/Event/requestUri */ ,
  /* ******************************************** */
  /* User attributes and fields */
  val userIP: String /* /root/Event/user/ip */ , val userAgent: String /* /root/Event/user/userAgent */ ,
  val userIsRobot: Boolean /* /root/Event/user/robot */ , val userLocation: String /* /root/Event/user/location */ ,
  val userBrowser: String /* /root/Event/user/browser */ ,
  /* ******************************************** */
  /* Search attributes and fields */
  val searchId: String /* /root/Event/search/searchId */ ,
  val searchWhat: String /* /root/Event/search/what */ , val searchWhere: String /* /root/Event/search/where */ ,
  val searchResultCount: String /* /root/Event/search/resultCount */ ,
  val searchWhatResolved: String /* /root/Event/search/resolvedWhat */ ,
  val searchIsDisambiguation: Boolean /* /root/Event/search/disambiguationPopup */ ,
  val searchIsSuggestion: Boolean /* /root/Event/search/dYMSuggestions */ ,
  val searchFailedOrSuccess: String /* /root/Event/search/failedOrSuccess */ ,
  val searchHasRHSListings: Boolean /* /root/Event/search/hasRHSListings */ ,
  val searchHasNonAdRollupListings: Boolean /* /root/Event/search/hasNonAdRollupListings */ ,
  val searchIsCalledBing: Boolean /* /root/Event/search/calledBing */ , val searchGeoOrDir: String /* /root/Event/search/geoORdir */ ,
  val categoryId: String /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id */ ,
  val tierId: String /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id */ ,
  val tierCount: Long /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count */ ,
  val searchGeoName: String /* /root/Event/search/matchedGeo/geo */ ,
  val searchGeoType: String /* /root/Event/search/matchedGeo/type */ ,
  val searchGeoPolygonIds: String /* /root/Event/search/matchedGeo/polygonIds */ ,
  val tierUdacCountList: String /* /root/Event/search/allListingsTypesMainLists */ ,
  val directoryIdList: String /* /root/Event/search/directoriesReturned */ ,
  val headingId: Long /* /root/Event/search/allHeadings/heading/name */ ,
  val headingRelevance: Char /* 'A' or 'B'*/ /* /root/Event/search/allHeadings/heading/category */ ,
  val searchType: String /* /root/Event/search/type */ , val searchResultPage: String /* /root/Event/search/resultPage */ ,
  val searchResultPerPage: String /* /root/Event/search/resultPerPage */ , val searchLatitude: String /* /root/Event/search/latitude */ ,
  val searchLongitude: String /* /root/Event/search/longitude */ ,
  /* ******************************************** */
  /* Merchants attributes and fields */
  val merchantId: String /* /root/Event/search/merchants/@id */ ,
  val merchantZone: String /* /root/Event/search/merchants/@zone */ ,
  val merchantLatitude: String /* /root/Event/search/merchants/@latitude */ ,
  val merchantLongitude: String, /* /root/Event/search/merchants/@longitude */
  val merchantDistance: String, /* /root/Event/search/merchants/@distance */ // TODO: when merchants are de-normalized, this field should be Long
  val merchantDisplayPosition: String /* /root/Event/search/merchants/RHSorLHS */ ,
  val merchantIsNonAdRollup: String /* /root/Event/search/merchants/isNonAdRollup */ , // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantRank: String /* /root/Event/search/merchants/ranking */ , // TODO: when merchants are de-normalized, this field should be Int
  val merchantIsRelevantListing: String /* /root/Event/search/merchants/isListingRelevant */ , // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantIsRelevantHeading: String /* /root/Event/search/merchants/entry/heading/@isRelevant */ , // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantHeadingIdList: String /* /root/Event/search/merchants/entry/heading/categories */ ,
  val merchantChannel1List: String /* /root/Event/search/merchants/entry/directories/channel1 */ ,
  val merchantChannel2List: String /* /root/Event/search/merchants/entry/directories/channel2 */ ,
  val productType: String /* /root/Event/search/merchants/entry/product/productType */ ,
  val productLanguage: String /* /root/Event/search/merchants/entry/product/language */ ,
  val productUdac: String /* /root/Event/search/merchants/entry/product/udac */ ,
  val merchantListingType: String /* /root/Event/search/merchants/entry/listingType */ ,
  /* ******************************************** */
  /* Search Analytics/Analysis attributes and fields */
  val searchAnalysisIsfuzzy: Boolean /* /root/Event/search/searchAnalysis/fuzzy */ ,
  val searchAnalysisIsGeoExpanded: Boolean /* /root/Event/search/searchAnalysis/geoExpanded */ ,
  val searchAnalysisIsBusinessName: Boolean /* /root/Event/search/searchAnalysis/businessName*/ ,
  val key: String /* /root/Event/searchAnalytics/entry/@key */ ,
  val value: String /* /root/Event/searchAnalytics/entry/@value */ ) extends Product with Serializable {
  override def toString = {
    s"""
         | eventId (/root/Event/@id) = ${eventId},
         | timestamp (/root/Event/@timestamp) = ${timestamp},
         | eventSite (/root/Event/@site) = ${eventSite},
         | eventSiteLanguage (/root/Event/@siteLanguage) = ${eventSiteLanguage},
         | userId (/root/Event/@userId) = ${userId},
         | apiKey (/root/Event/apiKey) = ${apiKey},
         | userSessionId (/root/Event/sessionId) = ${userSessionId},
         | transactionDuration (/root/Event/transactionDuration) = ${transactionDuration},
         | isResultCached (/root/Event/cachingUsed) = ${isResultCached},
         | eventReferrer (/root/Event/referrer) = ${eventReferrer},
         | pageName (/root/Event/pageName) = ${pageName},
         |  requestUri (/root/Event/requestUri) = ${requestUri},
         |  userIP (/root/Event/user/ip) = ${userIP},
         |  userAgent (/root/Event/user/userAgent) = ${userAgent},
         |  userIsRobot (/root/Event/user/robot) = ${userIsRobot},
         |  userLocation (/root/Event/user/location) = ${userLocation},
         |  userBrowser (/root/Event/user/browser) = ${userBrowser},
         |  searchId (/root/Event/search/searchId) = ${searchId},
         |  searchWhat (/root/Event/search/what) = ${searchWhat},
         |  searchWhere (/root/Event/search/where) = ${searchWhere},
         |  searchResultCount (/root/Event/search/resultCount) = ${searchResultCount},
         |  searchWhatResolved (/root/Event/search/resolvedWhat) = ${searchWhatResolved},
         |  searchIsDisambiguation (/root/Event/search/disambiguationPopup) = ${searchIsDisambiguation},
         |  searchIsSuggestion (/root/Event/search/dYMSuggestions) = ${searchIsSuggestion},
         |  searchFailedOrSuccess (/root/Event/search/failedOrSuccess) = ${searchFailedOrSuccess},
         |  searchHasRHSListings (/root/Event/search/hasRHSListings) = ${searchHasRHSListings},
         | searchHasNonAdRollupListings (/root/Event/search/hasNonAdRollupListings) = ${searchHasNonAdRollupListings},
         | searchIsCalledBing (/root/Event/search/calledBing) = ${searchIsCalledBing},
         | searchGeoOrDir (/root/Event/search/geoORdir) = ${searchGeoOrDir},
         | categoryId (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id) = ${categoryId},
         | tierId (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id) = ${tierId},
         | tierCount (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count) = ${tierCount},
         | searchGeoName (/root/Event/search/matchedGeo/geo) = ${searchGeoName},
         | searchGeoType (/root/Event/search/matchedGeo/type) = ${searchGeoType},
         | searchGeoPolygonIds (/root/Event/search/matchedGeo/polygonIds) = ${searchGeoPolygonIds},
         | tierUdacCountList (/root/Event/search/allListingsTypesMainLists) = ${tierUdacCountList},
         | directoryIdList (/root/Event/search/directoriesReturned) = ${directoryIdList},
         | headingId (/root/Event/search/allHeadings/heading/name) = ${headingId},
         | headingRelevance (/root/Event/search/allHeadings/heading/category) = ${headingRelevance},
         | searchType (/root/Event/search/type) = ${searchType},
         | searchResultPage (/root/Event/search/resultPage) = ${searchResultPage},
         | searchResultPerPage (/root/Event/search/resultPerPage) = ${searchResultPerPage},
         | searchLatitude (/root/Event/search/latitude) = ${searchLatitude},
         | searchLongitude (/root/Event/search/longitude) = ${searchLongitude},
         | merchantId (/root/Event/search/merchants/@id) = ${merchantId},
         | merchantZone (/root/Event/search/merchants/@zone) = ${merchantZone},
         | merchantLatitude (/root/Event/search/merchants/@latitude) = ${merchantLatitude},
         | merchantLongitude (/root/Event/search/merchants/@longitude) = ${merchantLongitude},
         | merchantDistance (/root/Event/search/merchants/@distance) = ${merchantDistance},
         | merchantDisplayPosition (/root/Event/search/merchants/RHSorLHS) = ${merchantDisplayPosition},
         | merchantIsNonAdRollup (/root/Event/search/merchants/isNonAdRollup) = ${merchantIsNonAdRollup},
         | merchantRank (/root/Event/search/merchants/ranking) = ${merchantRank},
         | merchantIsRelevantListing (/root/Event/search/merchants/isListingRelevant) = ${merchantIsRelevantListing},
         | merchantIsRelevantHeading (/root/Event/search/merchants/entry/heading/@isRelevant) = ${merchantIsRelevantHeading},
         | merchantHeadingIdList (/root/Event/search/merchants/entry/heading/categories) = ${merchantHeadingIdList},
         | merchantChannel1List (/root/Event/search/merchants/entry/directories/channel1) = ${merchantChannel1List},
         | merchantChannel2List (/root/Event/search/merchants/entry/directories/channel2) = ${merchantChannel2List},
         | productType (/root/Event/search/merchants/entry/product/productType) = ${productType},
         | productLanguage (/root/Event/search/merchants/entry/product/language) = ${productLanguage},
         | productUdac (/root/Event/search/merchants/entry/product/udac) = ${productUdac},
         | merchantListingType (/root/Event/search/merchants/entry/listingType) = ${merchantListingType},
         | searchAnalysisIsfuzzy (/root/Event/search/searchAnalysis/fuzzy) = ${searchAnalysisIsfuzzy},
         | searchAnalysisIsGeoExpanded (/root/Event/search/searchAnalysis/geoExpanded) = ${searchAnalysisIsGeoExpanded},
         | searchAnalysisIsBusinessName (/root/Event/search/searchAnalysis/businessName") = ${searchAnalysisIsBusinessName},
         | key (/root/Event/searchAnalytics/entry/@key) = ${key},
         | value (/root/Event/searchAnalytics/entry/@value) = ${value}
         """.stripMargin
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[eventDetail]

  override def productArity: Int = 66

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int) = n match {
    case 0 => eventId
    case 1 => timestamp
    case 2 => eventSite
    case 3 => eventSiteLanguage
    case 4 => userId
    case 5 => apiKey
    case 6 => userSessionId
    case 7 => transactionDuration
    case 8 => isResultCached
    case 9 => eventReferrer
    case 10 => pageName
    case 11 => requestUri
    case 12 => userIP
    case 13 => userAgent
    case 14 => userIsRobot
    case 15 => userLocation
    case 16 => userBrowser
    case 17 => searchId
    case 18 => searchWhat
    case 19 => searchWhere
    case 20 => searchResultCount
    case 21 => searchWhatResolved
    case 22 => searchIsDisambiguation
    case 23 => searchIsSuggestion
    case 24 => searchFailedOrSuccess
    case 25 => searchHasRHSListings
    case 26 => searchHasNonAdRollupListings
    case 27 => searchIsCalledBing
    case 28 => searchGeoOrDir
    case 29 => categoryId
    case 30 => tierId
    case 31 => tierCount
    case 32 => searchGeoName
    case 33 => searchGeoType
    case 34 => searchGeoPolygonIds
    case 35 => tierUdacCountList
    case 36 => directoryIdList
    case 37 => headingId
    case 38 => headingRelevance
    case 39 => searchType
    case 40 => searchResultPage
    case 41 => searchResultPerPage
    case 42 => searchLatitude
    case 43 => searchLongitude
    case 44 => merchantId
    case 45 => merchantZone
    case 46 => merchantLatitude
    case 47 => merchantLongitude
    case 48 => merchantDistance
    case 49 => merchantDisplayPosition
    case 50 => merchantIsNonAdRollup
    case 51 => merchantRank
    case 52 => merchantIsRelevantListing
    case 53 => merchantIsRelevantHeading
    case 54 => merchantHeadingIdList
    case 55 => merchantChannel1List
    case 56 => merchantChannel2List
    case 57 => productType
    case 58 => productLanguage
    case 59 => productUdac
    case 60 => merchantListingType
    case 61 => searchAnalysisIsfuzzy
    case 62 => searchAnalysisIsGeoExpanded
    case 63 => searchAnalysisIsBusinessName
    case 64 => key
    case 65 => value
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

}

object eventDetail {

  def runOrDefault[A, B](f: A => B)(default: B)(param: A): B = {
    try {
      f(param)
    } catch {
      case e: Exception =>
        default
    }
  }

  def list2String(l: List[String]): String = {
    l.length match {
      case 0 => ""
      case 1 => l.head
      case _ => l.mkString(start = "{", sep = ",", end = "}")
    }
  }

  /**
   * TODO
   * @param aMap
   * @return
   */
  def apply(aMap: Map[String, List[String]]): List[eventDetail] = {
    def getOrEmpty(fieldName: String): String = {
      list2String(aMap.getOrElse(fieldName, List.empty))
    }
    def displayTypeError(fieldNameInMap: String, dstType: String, resultFieldName: String): Unit = {
      val fieldValue = {
        val v = getOrEmpty(fieldNameInMap)
        if (v.isEmpty) "empty"
        else s"'${v}'"
      }
      // TODO: use 'logger'
      println(s"Field '${resultFieldName}' is supposed to be '${dstType}', but '${fieldNameInMap}' is ${fieldValue}")
    }
    // Boolean specifics:
    def parseAsBoolean(fieldName: String): Option[Boolean] = {
      getOrEmpty(fieldName).trim.toUpperCase match {
        case "TRUE" | "1" | "T" | "TRUE" | "Y" | "YES" => Some(true)
        case "FALSE" | "0" | "F" | "FALSE" | "N" | "NO" => Some(false)
        case x if (x.isEmpty) => Some(false) // TODO: if field not there, assuming false. Is this correct?
        case _ => None
      }
    }
    def displayBooleanError(fieldNameInMap: String, resultFieldName: String): Unit = {
      displayTypeError(fieldNameInMap, dstType = "Boolean", resultFieldName)
    }
    def parseAsBooleanOrFalse(fieldNameInMap: String, resultFieldName: String): Boolean = {
      parseAsBoolean(fieldNameInMap).getOrElse { displayBooleanError(fieldNameInMap, resultFieldName); false }
    }
    // Long specifics:
    def parseAsLong(fieldName: String): Option[Long] = {
      getOrEmpty(fieldName) match {
        case s if s.isEmpty => Some(0L) // TODO: if field not there, assuming 0L. Is this correct?
        case s => catching(classOf[Exception]) opt { s.toLong }
      }
    }
    def displayLongError(fieldNameInMap: String, resultFieldName: String): Unit = {
      displayTypeError(fieldNameInMap, dstType = "Long", resultFieldName)
    }
    def parseAsLongOrDefault(fieldNameInMap: String, resultFieldName: String): Long = {
      parseAsLong(fieldNameInMap).getOrElse { displayLongError(fieldNameInMap, resultFieldName); Long.MinValue }
    }
    val headingsWithCats = aMap.getOrElse("/root/Event/search/allHeadings/heading/name", List("")) zip aMap.getOrElse("/root/Event/search/allHeadings/heading/category", List(""))
    headingsWithCats.foldLeft(List.empty[eventDetail]) {
      case (listOfEvents, (aHeading, itsCategory)) =>
        new eventDetail(
          headingId = runOrDefault[String, Long] { _.toLong }(-1L)(aHeading),
          headingRelevance = runOrDefault[String, Char] { s => s.charAt(0) }('X')(itsCategory),
          eventId = getOrEmpty("/root/Event/@id"),
          timestamp = getOrEmpty("/root/Event/@timestamp"),
          eventSite = getOrEmpty("/root/Event/@site"),
          eventSiteLanguage = getOrEmpty("/root/Event/@siteLanguage"),
          userId = getOrEmpty("/root/Event/@userId"),
          apiKey = getOrEmpty("/root/Event/apiKey"),
          userSessionId = getOrEmpty("/root/Event/sessionId"),
          transactionDuration = runOrDefault[String, Long] { _.toLong }(-1)(getOrEmpty("/root/Event/transactionDuration")),
          isResultCached = parseAsBooleanOrFalse("/root/Event/cachingUsed", "isResultCached"),
          eventReferrer = getOrEmpty("/root/Event/referrer"),
          pageName = getOrEmpty("/root/Event/pageName"),
          requestUri = getOrEmpty("/root/Event/requestUri"),
          /* ******************************************** */
          /* User attributes and fields */
          userIP = getOrEmpty("/root/Event/user/ip"),
          userAgent = getOrEmpty("/root/Event/user/userAgent"),
          userIsRobot = parseAsBooleanOrFalse("/root/Event/user/robot", "userIsRobot"),
          userLocation = getOrEmpty("/root/Event/user/location"),
          userBrowser = getOrEmpty("/root/Event/user/browser"),
          /* ******************************************** */
          /* Search attributes and fields */
          searchId = getOrEmpty("/root/Event/search/searchId"),
          searchWhat = getOrEmpty("/root/Event/search/what"),
          searchWhere = getOrEmpty("/root/Event/search/where"),
          searchResultCount = getOrEmpty("/root/Event/search/resultCount"),
          searchWhatResolved = getOrEmpty("/root/Event/search/resolvedWhat"),
          searchIsDisambiguation = parseAsBooleanOrFalse("/root/Event/search/disambiguationPopup", "searchIsDisambiguation"),
          searchIsSuggestion = parseAsBooleanOrFalse("/root/Event/search/dYMSuggestions", "searchIsSuggestion"),
          searchFailedOrSuccess = getOrEmpty("/root/Event/search/failedOrSuccess"),
          searchHasRHSListings = parseAsBooleanOrFalse("/root/Event/search/hasRHSListings", "searchHasRHSListings"),
          searchHasNonAdRollupListings = parseAsBooleanOrFalse("/root/Event/search/hasNonAdRollupListings", "searchHasNonAdRollupListings"),
          searchIsCalledBing = parseAsBooleanOrFalse("/root/Event/search/calledBing", "searchIsCalledBing"),
          searchGeoOrDir = getOrEmpty("/root/Event/search/geoORdir"),
          categoryId = getOrEmpty("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id"),
          tierId = getOrEmpty("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id"),
          tierCount = parseAsLongOrDefault("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count", "tierCount"),
          searchGeoName = getOrEmpty("/root/Event/search/matchedGeo/geo"),
          searchGeoType = getOrEmpty("/root/Event/search/matchedGeo/type"),
          searchGeoPolygonIds = getOrEmpty("/root/Event/search/matchedGeo/polygonIds"),
          tierUdacCountList = getOrEmpty("/root/Event/search/allListingsTypesMainLists"),
          directoryIdList = getOrEmpty("/root/Event/search/directoriesReturned"),
          searchType = getOrEmpty("/root/Event/search/type"), searchResultPage = getOrEmpty("/root/Event/search/resultPage"),
          searchResultPerPage = list2String(aMap.getOrElse("/root/Event/search/resultPerPage", List.empty)), searchLatitude = getOrEmpty("/root/Event/search/latitude"),
          searchLongitude = getOrEmpty("/root/Event/search/longitude"),
          merchantId = getOrEmpty("/root/Event/search/merchants/@id"),
          merchantZone = getOrEmpty("/root/Event/search/merchants/@zone"),
          merchantLatitude = getOrEmpty("/root/Event/search/merchants/@latitude"),
          merchantLongitude = getOrEmpty("/root/Event/search/merchants/@longitude"),
          merchantDistance = getOrEmpty("/root/Event/search/merchants/@distance"),
          merchantDisplayPosition = getOrEmpty("/root/Event/search/merchants/RHSorLHS"),
          merchantIsNonAdRollup = getOrEmpty("/root/Event/search/merchants/isNonAdRollup"),
          merchantRank = getOrEmpty("/root/Event/search/merchants/ranking"),
          merchantIsRelevantListing = getOrEmpty("/root/Event/search/merchants/isListingRelevant"),
          merchantIsRelevantHeading = getOrEmpty("/root/Event/search/merchants/entry/heading/@isRelevant"),
          merchantHeadingIdList = getOrEmpty("/root/Event/search/merchants/entry/heading/categories"),
          merchantChannel1List = getOrEmpty("/root/Event/search/merchants/entry/directories/channel1"),
          merchantChannel2List = getOrEmpty("/root/Event/search/merchants/entry/directories/channel2"),
          productType = getOrEmpty("/root/Event/search/merchants/entry/product/productType"),
          productLanguage = getOrEmpty("/root/Event/search/merchants/entry/product/language"),
          productUdac = getOrEmpty("/root/Event/search/merchants/entry/product/udac"),
          merchantListingType = getOrEmpty("/root/Event/search/merchants/entry/listingType"),
          searchAnalysisIsfuzzy = parseAsBooleanOrFalse("/root/Event/search/searchAnalysis/fuzzy", "searchAnalysisIsfuzzy"),
          searchAnalysisIsGeoExpanded = parseAsBooleanOrFalse("/root/Event/search/searchAnalysis/geoExpanded", "searchAnalysisIsGeoExpanded"),
          searchAnalysisIsBusinessName = parseAsBooleanOrFalse("/root/Event/search/searchAnalysis/businessName", "searchAnalysisIsBusinessName"),
          /* ******************************************** */
          /* Search Analytics attributes and fields */
          key = getOrEmpty("/root/Event/searchAnalytics/entry/@key"),
          value = getOrEmpty("/root/Event/searchAnalytics/entry/@value")) :: listOfEvents
    }

  }
}

// TODO: replace 'println's by Spark logs writing
object Parser {
  import canpipe.parser.Base.{ CanPipeParser => BasicParser }

  def parseEventGroup(events: EventGroup, filterRules: Set[FilterRule]): RDD[eventDetail] = {
    val rddEventsRaw =
      events.eventsAsString.map { anEventAsString =>
        Source.fromString(anEventAsString) match {
          case x if (x == null) => {
            println(s"'Error building a Source from a particular XML event")
            Map.empty[String, scala.List[String]]
          }
          case theSource => {
            val rddMap = BasicParser.parseEvent(xml = new XMLEventReader(theSource), startXPath = XPath("/root/Event"), eventIdOpt = None)
            rddMap
          }
        }
      }
    rddEventsRaw.flatMap(eventDetail(_))
  }

}

class Parser(val filterRules: Set[FilterRule]) {
  def this() = this(Set.empty)
  def parseEventGroup(events: EventGroup) = Parser.parseEventGroup(events, filterRules)
}

