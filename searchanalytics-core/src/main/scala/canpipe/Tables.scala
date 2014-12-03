package canpipe

import canpipe.xml.XMLFields._
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.xml.{ Field => XMLField }
import util.xml.Base._ // among other things, brings implicits into scope

import scala.util.control.Exception._

/**
 * TODO: doc.
 */
case class Tables(anXMLNode: CanpipeXMLElem) {

  val (events: List[EventDetail], headings: List[EventsHeadingsAssociation], directories: List[EventsDirectoriesAssociation]) = {
    ({
      def getOrEmpty(anXMLField: XMLField): String = {
        (anXMLNode.value \ anXMLField.asList) match {
          case l if l.isEmpty => ""
          case l if (l.tail.isEmpty) => l.head
          case l => l.mkString(start = "{", sep = ",", end = "}")
        }
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
        (anXMLNode.value \ SEARCH_HEADING_NAME.asList) match {
          case l if l.isEmpty => (List(-1), List("")) // TODO: if no heading, spit -1L. That OK?
          case _ => ((anXMLNode.value \ SEARCH_HEADING_NAME.asList).map(_.toLong), anXMLNode.value \ SEARCH_HEADINGRELEVANCE.asList)
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
            EventDetail.sanityCheck(
              fillEventDetailWithPrototype(firstEd, theTimestamp = getOrEmpty(EVENT_TIMESTAMP), aDirectoryId, aHeading, itsCategory)) :: listOfEventOpts
        }.flatten
      r

    }, List.empty[EventsHeadingsAssociation], List.empty[EventsDirectoriesAssociation])
  }

}

