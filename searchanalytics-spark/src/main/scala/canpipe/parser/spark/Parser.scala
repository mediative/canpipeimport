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
  val eventTimestamp: String, /* /root/Event/@timestamp */
  val timestampId: Long, /* FK to Time table */
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
  val directoryId: Long /* decomposition of /root/Event/search/directoriesReturned */ ,
  val headingId: Long /* /root/Event/search/allHeadings/heading/name */ ,
  val headingRelevance: String /* 'A' or 'B'*/ /* /root/Event/search/allHeadings/heading/category */ , // TODO: put this as Char. Spark had problems with it - sove them! scala.MatchError: scala.Char (of class scala.reflect.internal.Types$TypeRef$$anon$6)
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
         | eventTimestamp (/root/Event/@timestamp) = ${eventTimestamp},
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
         | directoryId (/root/Event/search/directoriesReturned) = ${directoryId},
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

  override def productArity: Int = 67

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int) = n match {
    case 0 => eventId
    case 1 => eventTimestamp
    case 2 => timestampId
    case 3 => eventSite
    case 4 => eventSiteLanguage
    case 5 => userId
    case 6 => apiKey
    case 7 => userSessionId
    case 8 => transactionDuration
    case 9 => isResultCached
    case 10 => eventReferrer
    case 11 => pageName
    case 12 => requestUri
    case 13 => userIP
    case 14 => userAgent
    case 15 => userIsRobot
    case 16 => userLocation
    case 17 => userBrowser
    case 18 => searchId
    case 19 => searchWhat
    case 20 => searchWhere
    case 21 => searchResultCount
    case 22 => searchWhatResolved
    case 23 => searchIsDisambiguation
    case 24 => searchIsSuggestion
    case 25 => searchFailedOrSuccess
    case 26 => searchHasRHSListings
    case 27 => searchHasNonAdRollupListings
    case 28 => searchIsCalledBing
    case 29 => searchGeoOrDir
    case 30 => categoryId
    case 31 => tierId
    case 32 => tierCount
    case 33 => searchGeoName
    case 34 => searchGeoType
    case 35 => searchGeoPolygonIds
    case 36 => tierUdacCountList
    case 37 => directoryId
    case 38 => headingId
    case 39 => headingRelevance
    case 40 => searchType
    case 41 => searchResultPage
    case 42 => searchResultPerPage
    case 43 => searchLatitude
    case 44 => searchLongitude
    case 45 => merchantId
    case 46 => merchantZone
    case 47 => merchantLatitude
    case 48 => merchantLongitude
    case 49 => merchantDistance
    case 50 => merchantDisplayPosition
    case 51 => merchantIsNonAdRollup
    case 52 => merchantRank
    case 53 => merchantIsRelevantListing
    case 54 => merchantIsRelevantHeading
    case 55 => merchantHeadingIdList
    case 56 => merchantChannel1List
    case 57 => merchantChannel2List
    case 58 => productType
    case 59 => productLanguage
    case 60 => productUdac
    case 61 => merchantListingType
    case 62 => searchAnalysisIsfuzzy
    case 63 => searchAnalysisIsGeoExpanded
    case 64 => searchAnalysisIsBusinessName
    case 65 => key
    case 66 => value
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
   *
   * TODO: use logger
   * @param e
   * @return
   */
  private[spark] def sanityCheck(e: eventDetail): Option[eventDetail] = {
    if (e.eventId.isEmpty) {
      // println("Event has an empty id")
      None
    } else
      Some(e)
  }

  /**
   * TODO
   * @param aMap
   * @return
   */
  def apply(aMap: Map[String, List[String]]): List[eventDetail] = {

    val mapOnlyEmpties = aMap.filter(_._1.isEmpty)
    if (!mapOnlyEmpties.isEmpty) {
      println("EMPTY FIELDS ========> ")
      mapOnlyEmpties.foreach { case (key, _) => println(key) }
    }
    // println("NON-EMPTY FIELDS ========> ")
    // aMap.filter(!_._1.isEmpty).foreach { case (key, value) => println(key) }

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

    val l = aMap.getOrElse("/root/Event/search/directoriesReturned", List(""))
    val dirIds = {
      if (l.head.trim.isEmpty) {
        Set(-1L) // TODO: NB = when directory does not match anything, the value in table will be -1L. That OK?
      } else {
        val dirsReturned = l.head
        // 'dirsReturned' look like this: 107553:One Toronto Core West,107556:One Toronto Core SE,107555:One Toronto Core Ctr,107554:One Toronto Core NE
        val DirectoryRegex = """(\d*):(.*)""".r // TODO: put this somewhere else
        dirsReturned.split(",").flatMap { aDir => catching(classOf[Exception]).opt { val DirectoryRegex(id, name) = aDir; id.toLong } }.toSet
      }
    }
    val headingsNames = aMap.getOrElse("/root/Event/search/allHeadings/heading/name", List(""))
    val headingsCats = aMap.getOrElse("/root/Event/search/allHeadings/heading/category", List(""))
    val headingsWithCats = (headingsNames zip headingsCats).toSet
    val dirsHeadingsAndCats = for (dir <- dirIds; headingAndCat <- headingsWithCats) yield (dir, headingAndCat)
    dirsHeadingsAndCats.foldLeft(List.empty[Option[eventDetail]]) {
      case (listOfEventOpts, (aDirectoryId, (aHeading, itsCategory))) =>
        sanityCheck(new eventDetail(
          headingId = runOrDefault[String, Long] { _.toLong }(-1L)(aHeading),
          headingRelevance = itsCategory,
          eventId = getOrEmpty("/root/Event/@id"),
          eventTimestamp = getOrEmpty("/root/Event/@timestamp"),
          timestampId = parseAsLongOrDefault("/root/Event/timestampId", "timestampId"),
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
          directoryId = aDirectoryId, // decomposition of getOrEmpty("/root/Event/search/dirsReturned"),
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
          value = getOrEmpty("/root/Event/searchAnalytics/entry/@value"))) :: listOfEventOpts
    }.flatten
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
            BasicParser.parseEvent(xml = new XMLEventReader(theSource), startXPath = XPath("/root/Event"), eventIdOpt = None)
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

