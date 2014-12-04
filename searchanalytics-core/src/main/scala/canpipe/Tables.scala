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

  val (eventOpt: Option[EventDetail], headings: Set[EventsHeadingsAssociation], directories: Set[EventsDirectoriesAssociation]) = {
    {
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

      val theTimestamp = getOrEmpty(EVENT_TIMESTAMP)
      val DateFromXMLRegex = """(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).(\d\d\d)-(\d\d):(\d\d)""".r // TODO: put this somewhere else
      val theTimestampId =
        catching(classOf[Exception]).opt {
          val DateFromXMLRegex(year, month, day, hour, mins, secs, msecs, hourToGMT, minsToGMT) = theTimestamp
          year.toLong * 10000 + month.toLong * 100 + day.toLong
        }.getOrElse(-1L) // TODO: OK?

      val firstEd = new EventDetail(
        eventId = getOrEmpty(EVENT_ID),
        eventTimestamp = theTimestamp,
        timestampId = theTimestampId,
        eventSite = getOrEmpty(EVENT_SITE),
        eventSiteLanguage = getOrEmpty(EVENT_SITELANGUAGE),
        eventType = getOrEmpty(EVENT_TYPE),
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

      EventDetail.sanityCheck(firstEd) match {
        case None => (None, Set.empty, Set.empty)
        case Some(ed) => {
          (Some(ed),
            headingsWithCats.map { case (headId, cat) => EventsHeadingsAssociation(eventId = ed.eventId, headingId = headId, category = cat) },
            dirIds.map { dirId => EventsDirectoriesAssociation(eventId = ed.eventId, directoryId = dirId) })
        }
      }

    }
  }

}

