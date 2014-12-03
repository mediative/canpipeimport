package canpipe

import canpipe.xml.XMLFields

import scala.util.control.Exception._
import XMLFields._
import util.xml.{ Field => XMLField }
import canpipe.xml.{ Elem => CanpipeXMLElem }

class EventDetail(
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
  val isResultCached: Boolean /* /root/Event/cachingUsed */ ,
  val eventReferrer: String /* /root/Event/referrer */ ,
  val pageName: String /* /root/Event/pageName */ ,
  val requestUri: String /* /root/Event/requestUri */ ,
  /* ******************************************** */
  /* User attributes and fields */
  val userIP: String /* /root/Event/user/ip */ ,
  val userAgent: String /* /root/Event/user/userAgent */ ,
  val userIsRobot: Boolean /* /root/Event/user/robot */ ,
  val userLocation: String /* /root/Event/user/location */ ,
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

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EventDetail]

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

object EventDetail {

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
   * TODO: write this.
   * @param e
   * @return
   */
  private[canpipe] def sanityCheck(e: EventDetail): Option[EventDetail] = {
    // TODO: following structure sucks.
    val importantFields = List(
      ("eventId", e.eventId.isEmpty),
      ("eventTimestamp", e.eventTimestamp.isEmpty))
    importantFields.
      find { case (_, errorCondition) => errorCondition }.
      map { case (fieldName, _) => fieldName }.
      map { fieldName => println(s"${fieldName} is empty"); None }. // TODO: logger.error
      getOrElse(Some(e))
  }

  def getValues(n: scala.xml.NodeSeq, thePath: List[String]): List[String] = {
    thePath.length match {
      case 0 => List.empty
      case 1 => (n \ thePath.head).iterator.toList.map(_.text)
      case _ => (n \ thePath.head).iterator.toList.flatMap(n => getValues(n, thePath.tail))
    }
  }

  /**
   * TODO
   * @param aMap
   * @return
   */
  def apply(anXMLNode: CanpipeXMLElem): List[EventDetail] = {
    def getOrEmpty(anXMLField: XMLField): String = {
      list2String(getValues(anXMLNode.value, anXMLField.asList))
    }
    def displayTypeError(fieldNameInMap: XMLField, dstType: String, resultFieldName: String): Unit = {
      val fieldValue = {
        val v = getOrEmpty(fieldNameInMap)
        if (v.isEmpty) "empty"
        else s"'${v}'"
      }
      // TODO: use 'logger'
      println(s"Field '${resultFieldName}' is supposed to be '${dstType}', but '${fieldNameInMap}' is ${fieldValue}")
    }
    // Boolean specifics:
    def parseAsBoolean(fieldName: XMLField): Option[Boolean] = {
      getOrEmpty(fieldName).trim.toUpperCase match {
        case "TRUE" | "1" | "T" | "TRUE" | "Y" | "YES" => Some(true)
        case "FALSE" | "0" | "F" | "FALSE" | "N" | "NO" => Some(false)
        case x if (x.isEmpty) => Some(false) // TODO: if field not there, assuming false. Is this correct?
        case _ => None
      }
    }
    def displayBooleanError(fieldNameInMap: XMLField, resultFieldName: String): Unit = {
      displayTypeError(fieldNameInMap, dstType = "Boolean", resultFieldName)
    }
    def parseAsBooleanOrFalse(fieldNameInMap: XMLField, resultFieldName: String): Boolean = {
      parseAsBoolean(fieldNameInMap).getOrElse { displayBooleanError(fieldNameInMap, resultFieldName); false }
    }
    // Long specifics:
    def parseAsLong(fieldName: XMLField): Option[Long] = {
      getOrEmpty(fieldName) match {
        case s if s.isEmpty => Some(0L) // TODO: if field not there, assuming 0L. Is this correct?
        case s => catching(classOf[Exception]) opt { s.toLong }
      }
    }
    def displayLongError(fieldNameInMap: XMLField, resultFieldName: String): Unit = {
      displayTypeError(fieldNameInMap, dstType = "Long", resultFieldName)
    }
    def parseAsLongOrDefault(fieldNameInMap: XMLField, resultFieldName: String): Long = {
      parseAsLong(fieldNameInMap).getOrElse { displayLongError(fieldNameInMap, resultFieldName); Long.MinValue }
    }

    val dirsReturned = getOrEmpty(SEARCH_DIRECTORIESRETURNED)
    val dirIds = {
      if (dirsReturned.trim.isEmpty) {
        Set(-1L) // TODO: NB = when directory does not match anything, the value in table will be -1L. That OK?
      } else {
        // 'dirsReturned' look like this: 107553:One Toronto Core West,107556:One Toronto Core SE,107555:One Toronto Core Ctr,107554:One Toronto Core NE
        val DirectoryRegex = """(\d*):(.*)""".r // TODO: put this somewhere else
        dirsReturned.split(",").flatMap { aDir => catching(classOf[Exception]).opt { val DirectoryRegex(id, name) = aDir; id.toLong } }.toSet
      }
    }
    val (headingsIds, headingsCats): (List[Long], List[String]) =
      getValues(anXMLNode.value, SEARCH_HEADING_NAME.asList) match {
        case l if l.isEmpty => (List(-1), List("")) // TODO: if no heading, spit -1L. That OK?
        case _ => (getValues(anXMLNode.value, SEARCH_HEADING_NAME.asList).map(_.toLong), getValues(anXMLNode.value, SEARCH_HEADINGRELEVANCE.asList))
      }
    val headingsWithCats = (headingsIds zip headingsCats).toSet
    val dirsHeadingsAndCats = for (dir <- dirIds; headingAndCat <- headingsWithCats) yield (dir, headingAndCat)

    val firstEd = new EventDetail(
      eventId = getOrEmpty(EVENT_ID),
      eventTimestamp = "",
      timestampId = -1L,
      eventSite = getOrEmpty(EVENT_SITE),
      eventSiteLanguage = getOrEmpty(EVENT_SITELANGUAGE),
      // eventType = getOrEmpty(EVENT_TYPE), // TODO
      userId = getOrEmpty(EVENT_USER_ID),
      apiKey = getOrEmpty(EVENT_API_KEY),
      userSessionId = getOrEmpty(EVENT_USER_SESSIONID),
      transactionDuration = parseAsLongOrDefault(EVENT_TRANSACTION_DURATION, "transactionDuration"),
      isResultCached = parseAsBooleanOrFalse(EVENT_CACHINGUSED, "isResultCached"),
      eventReferrer = getOrEmpty(EVENT_REFERRER),
      pageName = getOrEmpty(EVENT_PAGENAME),
      requestUri = getOrEmpty(EVENT_REQUESTURI),
      /* ******************************************** */
      /* User attributes and fields */
      userIP = getOrEmpty(USER_IP),
      userAgent = getOrEmpty(USER_AGENT),
      userIsRobot = parseAsBooleanOrFalse(USER_ROBOT, "userIsRobot"),
      userLocation = getOrEmpty(USER_LOCATION),
      userBrowser = getOrEmpty(USER_BROWSER),
      /* ******************************************** */
      /* Search attributes and fields */
      searchId = getOrEmpty(SEARCH_ID),
      searchWhat = getOrEmpty(SEARCH_WHAT),
      searchWhere = getOrEmpty(SEARCH_WHERE),
      searchResultCount = getOrEmpty(SEARCH_RESULTCOUNT),
      searchWhatResolved = getOrEmpty(SEARCH_WHATRESOLVED),
      searchIsDisambiguation = parseAsBooleanOrFalse(SEARCH_DISAMBIGUATIONPOPUP, "searchIsDisambiguation"),
      searchIsSuggestion = parseAsBooleanOrFalse(SEARCH_DYMSUGGESTIONS, "searchIsSuggestion"),
      searchFailedOrSuccess = getOrEmpty(SEARCH_FAILEDORSUCCESS),
      searchHasRHSListings = parseAsBooleanOrFalse(SEARCH_HASRHSLISTINGS, "searchHasRHSListings"),
      searchHasNonAdRollupListings = parseAsBooleanOrFalse(SEARCH_HASNONADROLLUPLISTINGS, "searchHasNonAdRollupListings"),
      searchIsCalledBing = parseAsBooleanOrFalse(SEARCH_CALLEDBING, "searchIsCalledBing"),
      searchGeoOrDir = getOrEmpty(SEARCH_GEOORDIR),
      categoryId = getOrEmpty(SEARCH_CATEGORYID),
      tierId = getOrEmpty(SEARCH_TIERID),
      tierCount = parseAsLongOrDefault(SEARCH_TIERCOUNT, "tierCount"),
      searchGeoName = getOrEmpty(SEARCH_GEONAME),
      searchGeoType = getOrEmpty(SEARCH_GEOTYPE),
      searchGeoPolygonIds = getOrEmpty(SEARCH_GEOPOLYGONIDS),
      tierUdacCountList = getOrEmpty(SEARCH_TIERUDACCOUNTLIST),
      directoryId = -1L,
      headingId = -1L,
      headingRelevance = "",
      searchType = getOrEmpty(SEARCH_TYPE),
      searchResultPage = getOrEmpty(SEARCH_RESULTPAGE),
      searchResultPerPage = getOrEmpty(SEARCH_RESULTPERPAGE),
      searchLatitude = getOrEmpty(SEARCH_LATITUDE),
      searchLongitude = getOrEmpty(SEARCH_LONGITUDE),
      /* ******************************************** */
      /* Merchants attributes and fields */
      merchantId = getOrEmpty(MERCHANTS_ID),
      merchantZone = getOrEmpty(MERCHANTS_ZONE),
      merchantLatitude = getOrEmpty(MERCHANTS_LATITUDE),
      merchantLongitude = getOrEmpty(MERCHANTS_LONGITUDE),
      merchantDistance = getOrEmpty(MERCHANTS_DISTANCE),
      merchantDisplayPosition = getOrEmpty(MERCHANTS_RHS_OR_LHS),
      merchantIsNonAdRollup = getOrEmpty(MERCHANTS_NONADROLLUP),
      merchantRank = getOrEmpty(MERCHANTS_RANKING),
      merchantIsRelevantListing = getOrEmpty(MERCHANTS_IS_RELEVANT_LISTING),
      merchantIsRelevantHeading = getOrEmpty(MERCHANTS_IS_RELEVANT_HEADING),
      merchantHeadingIdList = getOrEmpty(MERCHANTS_HEADING_CATEGORIES),
      merchantChannel1List = getOrEmpty(MERCHANTS_DIRECTORIES_CHANNEL1),
      merchantChannel2List = getOrEmpty(MERCHANTS_DIRECTORIES_CHANNEL2),
      productType = getOrEmpty(MERCHANTS_PRODUCT_TYPE),
      productLanguage = getOrEmpty(MERCHANTS_PRODUCT_LANGUAGE),
      productUdac = getOrEmpty(MERCHANTS_PRODUCT_UDAC),
      merchantListingType = getOrEmpty(MERCHANTS_LISTING_TYPE),
      /* ******************************************** */
      /* Search Analytics/Analysis attributes and fields */
      searchAnalysisIsfuzzy = parseAsBooleanOrFalse(SEARCHANALYSIS_ISFUZZY, "searchAnalysisIsfuzzy"),
      searchAnalysisIsGeoExpanded = parseAsBooleanOrFalse(SEARCHANALYSIS_GEOEXPANDED, "searchAnalysisIsGeoExpanded"),
      searchAnalysisIsBusinessName = parseAsBooleanOrFalse(SEARCHANALYSIS_BUSINESSNAME, "searchAnalysisIsBusinessName"),
      key = getOrEmpty(SEARCHANALYTICS_KEYS),
      value = getOrEmpty(SEARCHANALYTICS_VALUES))

    def fillEventDetailWithPrototype(firstEd: EventDetail,
                                     theTimestamp: String,
                                     aDirectoryId: Long,
                                     aHeading: Long,
                                     itsCategory: String): EventDetail = {
      val DateFromXMLRegex = """(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).(\d\d\d)-(\d\d):(\d\d)""".r // TODO: put this somewhere else
      val theTimestampId =
        catching(classOf[Exception]).opt {
          val DateFromXMLRegex(year, month, day, hour, mins, secs, msecs, hourToGMT, minsToGMT) = theTimestamp
          year.toLong * 10000 + month.toLong * 100 + day.toLong
        }.getOrElse(-1L) // TODO: OK?

      new EventDetail(
        eventId = firstEd.eventId, //  commonMap.get(EVENT_ID).get, // getOrEmpty(EVENT_ID),
        eventTimestamp = theTimestamp,
        timestampId = theTimestampId,
        eventSite = firstEd.eventSite, //  commonMap.get(EVENT_SITE).get, // getOrEmpty(EVENT_SITE),
        eventSiteLanguage = firstEd.eventSiteLanguage, //  commonMap.get(EVENT_SITELANGUAGE).get, // getOrEmpty(EVENT_SITELANGUAGE),
        // eventType = getOrEmpty(EVENT_TYPE), // TODO
        userId = firstEd.userId, //  commonMap.get(EVENT_USER_ID).get, // getOrEmpty(EVENT_USER_ID),
        apiKey = firstEd.apiKey, //  commonMap.get(EVENT_API_KEY).get, // getOrEmpty(EVENT_API_KEY),
        userSessionId = firstEd.userSessionId, //  commonMap.get(EVENT_USER_SESSIONID).get, // getOrEmpty(EVENT_USER_SESSIONID),
        transactionDuration = firstEd.transactionDuration, //  parseAsLongOrDefault(EVENT_TRANSACTION_DURATION, "transactionDuration"),
        isResultCached = firstEd.isResultCached, //  parseAsBooleanOrFalse(EVENT_CACHINGUSED, "isResultCached"),
        eventReferrer = firstEd.eventReferrer, //  commonMap.get(EVENT_REFERRER).get, // getOrEmpty(EVENT_REFERRER),
        pageName = firstEd.pageName, //  commonMap.get(EVENT_PAGENAME).get, // getOrEmpty(EVENT_PAGENAME),
        requestUri = firstEd.requestUri, //  commonMap.get(EVENT_REQUESTURI).get, // getOrEmpty(EVENT_REQUESTURI),
        /* ******************************************** */
        /* User attributes and fields */
        userIP = firstEd.userIP, //  commonMap.get(USER_IP).get, // getOrEmpty(USER_IP),
        userAgent = firstEd.userAgent, //  commonMap.get(USER_AGENT).get, // getOrEmpty(USER_AGENT),
        userIsRobot = firstEd.userIsRobot, //  parseAsBooleanOrFalse(USER_ROBOT, "userIsRobot"),
        userLocation = firstEd.userLocation, //  commonMap.get(USER_LOCATION).get, // getOrEmpty(USER_LOCATION),
        userBrowser = firstEd.userBrowser, //  commonMap.get(USER_BROWSER).get, // getOrEmpty(USER_BROWSER),
        /* ******************************************** */
        /* Search attributes and fields */
        searchId = firstEd.searchId, //  commonMap.get(SEARCH_ID).get, // getOrEmpty(SEARCH_ID),
        searchWhat = firstEd.searchWhat, //  commonMap.get(SEARCH_WHAT).get, // getOrEmpty(SEARCH_WHAT),
        searchWhere = firstEd.searchWhere, //  commonMap.get(SEARCH_WHERE).get, // getOrEmpty(SEARCH_WHERE),
        searchResultCount = firstEd.searchResultCount, //  commonMap.get(SEARCH_RESULTCOUNT).get, // getOrEmpty(SEARCH_RESULTCOUNT),
        searchWhatResolved = firstEd.searchWhatResolved, //  commonMap.get(SEARCH_WHATRESOLVED).get, // getOrEmpty(SEARCH_WHATRESOLVED),
        searchIsDisambiguation = firstEd.searchIsDisambiguation, //  parseAsBooleanOrFalse(SEARCH_DISAMBIGUATIONPOPUP, "searchIsDisambiguation"),
        searchIsSuggestion = firstEd.searchIsSuggestion, //  parseAsBooleanOrFalse(SEARCH_DYMSUGGESTIONS, "searchIsSuggestion"),
        searchFailedOrSuccess = firstEd.searchFailedOrSuccess, //  commonMap.get(SEARCH_FAILEDORSUCCESS).get, // getOrEmpty(SEARCH_FAILEDORSUCCESS),
        searchHasRHSListings = firstEd.searchHasRHSListings, //  parseAsBooleanOrFalse(SEARCH_HASRHSLISTINGS, "searchHasRHSListings"),
        searchHasNonAdRollupListings = firstEd.searchHasNonAdRollupListings, //  parseAsBooleanOrFalse(SEARCH_HASNONADROLLUPLISTINGS, "searchHasNonAdRollupListings"),
        searchIsCalledBing = firstEd.searchIsCalledBing, //  parseAsBooleanOrFalse(SEARCH_CALLEDBING, "searchIsCalledBing"),
        searchGeoOrDir = firstEd.searchGeoOrDir, //  commonMap.get(SEARCH_GEOORDIR).get, // getOrEmpty(SEARCH_GEOORDIR),
        categoryId = firstEd.categoryId, //  commonMap.get(SEARCH_CATEGORYID).get, // getOrEmpty(SEARCH_CATEGORYID),
        tierId = firstEd.tierId, //  commonMap.get(SEARCH_TIERID).get, // getOrEmpty(SEARCH_TIERID),
        tierCount = firstEd.tierCount, //  parseAsLongOrDefault(SEARCH_TIERCOUNT, "tierCount"),
        searchGeoName = firstEd.searchGeoName, //  commonMap.get(SEARCH_GEONAME).get, // getOrEmpty(SEARCH_GEONAME),
        searchGeoType = firstEd.searchGeoType, //  commonMap.get(SEARCH_GEOTYPE).get, // getOrEmpty(SEARCH_GEOTYPE),
        searchGeoPolygonIds = firstEd.searchGeoPolygonIds, //  commonMap.get(SEARCH_GEOPOLYGONIDS).get, // getOrEmpty(SEARCH_GEOPOLYGONIDS),
        tierUdacCountList = firstEd.tierUdacCountList, //  commonMap.get(SEARCH_TIERUDACCOUNTLIST).get, // getOrEmpty(SEARCH_TIERUDACCOUNTLIST),
        directoryId = aDirectoryId,
        headingId = aHeading,
        headingRelevance = itsCategory,
        searchType = firstEd.searchType, //  commonMap.get(SEARCH_TYPE).get, // getOrEmpty(SEARCH_TYPE),
        searchResultPage = firstEd.searchResultPage, //  commonMap.get(SEARCH_RESULTPAGE).get, // getOrEmpty(SEARCH_RESULTPAGE),
        searchResultPerPage = firstEd.searchResultPerPage, //  commonMap.get(SEARCH_RESULTPERPAGE).get, // getOrEmpty(SEARCH_RESULTPERPAGE),
        searchLatitude = firstEd.searchLatitude, //  commonMap.get(SEARCH_LATITUDE).get, // getOrEmpty(SEARCH_LATITUDE),
        searchLongitude = firstEd.searchLongitude, //  commonMap.get(SEARCH_LONGITUDE).get, // getOrEmpty(SEARCH_LONGITUDE),
        /* ******************************************** */
        /* Merchants attributes and fields */
        merchantId = firstEd.merchantId, //  commonMap.get(MERCHANTS_ID).get, // getOrEmpty(MERCHANTS_ID),
        merchantZone = firstEd.merchantZone, //  commonMap.get(MERCHANTS_ZONE).get, // getOrEmpty(MERCHANTS_ZONE),
        merchantLatitude = firstEd.merchantLatitude, //  commonMap.get(MERCHANTS_LATITUDE).get, // getOrEmpty(MERCHANTS_LATITUDE),
        merchantLongitude = firstEd.merchantLongitude, //  commonMap.get(MERCHANTS_LONGITUDE).get, // getOrEmpty(MERCHANTS_LONGITUDE),
        merchantDistance = firstEd.merchantDistance, //  commonMap.get(MERCHANTS_DISTANCE).get, // getOrEmpty(MERCHANTS_DISTANCE),
        merchantDisplayPosition = firstEd.merchantDisplayPosition, //  commonMap.get(MERCHANTS_RHS_OR_LHS).get, // getOrEmpty(MERCHANTS_RHS_OR_LHS),
        merchantIsNonAdRollup = firstEd.merchantIsNonAdRollup, //  commonMap.get(MERCHANTS_NONADROLLUP).get, // getOrEmpty(MERCHANTS_NONADROLLUP),
        merchantRank = firstEd.merchantRank, //  commonMap.get(MERCHANTS_RANKING).get, // getOrEmpty(MERCHANTS_RANKING),
        merchantIsRelevantListing = firstEd.merchantIsRelevantListing, //  commonMap.get(MERCHANTS_IS_RELEVANT_LISTING).get, // getOrEmpty(MERCHANTS_IS_RELEVANT_LISTING),
        merchantIsRelevantHeading = firstEd.merchantIsRelevantHeading, //  commonMap.get(MERCHANTS_IS_RELEVANT_HEADING).get, // getOrEmpty(MERCHANTS_IS_RELEVANT_HEADING),
        merchantHeadingIdList = firstEd.merchantHeadingIdList, //  commonMap.get(MERCHANTS_HEADING_CATEGORIES).get, // getOrEmpty(MERCHANTS_HEADING_CATEGORIES),
        merchantChannel1List = firstEd.merchantChannel1List, //  commonMap.get(MERCHANTS_DIRECTORIES_CHANNEL1).get, // getOrEmpty(MERCHANTS_DIRECTORIES_CHANNEL1),
        merchantChannel2List = firstEd.merchantChannel2List, //  commonMap.get(MERCHANTS_DIRECTORIES_CHANNEL2).get, // getOrEmpty(MERCHANTS_DIRECTORIES_CHANNEL2),
        productType = firstEd.productType, //  commonMap.get(MERCHANTS_PRODUCT_TYPE).get, // getOrEmpty(MERCHANTS_PRODUCT_TYPE),
        productLanguage = firstEd.productLanguage, //  commonMap.get(MERCHANTS_PRODUCT_LANGUAGE).get, // getOrEmpty(MERCHANTS_PRODUCT_LANGUAGE),
        productUdac = firstEd.productUdac, //  commonMap.get(MERCHANTS_PRODUCT_UDAC).get, // getOrEmpty(MERCHANTS_PRODUCT_UDAC),
        merchantListingType = firstEd.merchantListingType, //  commonMap.get(MERCHANTS_LISTING_TYPE).get, // getOrEmpty(MERCHANTS_LISTING_TYPE),
        /* ******************************************** */
        /* Search Analytics/Analysis attributes and fields */
        searchAnalysisIsfuzzy = firstEd.searchAnalysisIsfuzzy, //  parseAsBooleanOrFalse(SEARCHANALYSIS_ISFUZZY, "searchAnalysisIsfuzzy"),
        searchAnalysisIsGeoExpanded = firstEd.searchAnalysisIsGeoExpanded, //  parseAsBooleanOrFalse(SEARCHANALYSIS_GEOEXPANDED, "searchAnalysisIsGeoExpanded"),
        searchAnalysisIsBusinessName = firstEd.searchAnalysisIsBusinessName, //  parseAsBooleanOrFalse(SEARCHANALYSIS_BUSINESSNAME, "searchAnalysisIsBusinessName"),
        key = firstEd.key, //  commonMap.get(SEARCHANALYTICS_KEYS).get, //  getOrEmpty(SEARCHANALYTICS_KEYS),
        value = firstEd.value //  commonMap.get(SEARCHANALYTICS_VALUES).get // getOrEmpty(SEARCHANALYTICS_VALUES))
        )

    }

    val r =
      dirsHeadingsAndCats.foldLeft(List.empty[Option[EventDetail]]) {
        case (listOfEventOpts, (aDirectoryId, (aHeading, itsCategory))) =>
          sanityCheck(
            fillEventDetailWithPrototype(firstEd, theTimestamp = getOrEmpty(EVENT_TIMESTAMP), aDirectoryId, aHeading, itsCategory)) :: listOfEventOpts
      }.flatten
    r
  }
}

