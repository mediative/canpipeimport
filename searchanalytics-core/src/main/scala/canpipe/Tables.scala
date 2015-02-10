package canpipe

import canpipe.EventDetail._
import canpipe.xml.XMLFields._
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.{ ErrorLogging, Logging }
import util.errorhandler.Base.GenericHandler
import util.types.reader.Base.ReadSym
import util.xml.{ Field => XMLField }
import util.xml.Base._ // among other things, brings implicits into scope

import scala.util.control.Exception._

/**
 * TODO: doc.
 */
case class Tables(anXMLNode: CanpipeXMLElem) {

  val (events: Option[EventDetail], headings: Set[EventsHeadingsAssociation], directories: Set[EventsDirectoriesAssociation]) = {
    {
      def getOrEmpty(anXMLField: XMLField): String = {
        (anXMLNode.value \ anXMLField.asList).mkString(",")
      }

      // Readers helpers:
      object FieldReader extends ErrorLogging {
        import util.errorhandler.Base.{ LongHandler, IntHandler, DoubleHandler, BooleanHandler }
        import util.types.reader.Base.{ LongReader, IntReader, DoubleReader, BooleanReader }
        def read[T](field: util.xml.Field)(implicit theReader: ReadSym[T], errHandler: GenericHandler[T]): T = {
          val fieldValue = getOrEmpty(field)
          errHandler.handle(theReader.read(fieldValue)).left.map { aMsg => s"${field.asString} incorrectly set to '${fieldValue}': ${aMsg}" } match {
            case Left(errMsg) =>
              logger.debug(s"${errMsg} (returning default value '${theReader.default}'})")
              theReader.default
            case Right(v) => v
          }
        }
      }

      /* Normalize Directories */
      val dirsReturned = getOrEmpty(directoriesReturned)
      val dirIds = {
        if (dirsReturned.trim.isEmpty) {
          Set(-1L) // TODO: NB = when directory does not match anything, the value in table will be -1L. That OK?
        } else {
          // 'dirsReturned' look like this: 107553:One Toronto Core West,107556:One Toronto Core SE,107555:One Toronto Core Ctr,107554:One Toronto Core NE
          val DirectoryRegex = """(\d*):(.*)""".r // TODO: put this somewhere else
          dirsReturned.split(",").flatMap { aDir => catching(classOf[Exception]).opt { val DirectoryRegex(id, name) = aDir; id.toLong } }.toSet
        }
      }
      /* Normalize Headings */
      val (headingsIds, headingsCats): (Seq[Long], Seq[String]) =
        (anXMLNode.value \ headingId.asList) match {
          case l if l.isEmpty => (List(-1), List("")) // TODO: if no heading, spit -1L. That OK?
          case _ => ((anXMLNode.value \ headingId.asList).map(_.toLong), (anXMLNode.value \ headingRelevance.asList))
        }
      val headingsWithCats = (headingsIds zip headingsCats).toSet

      BasicInfo(
        id = getOrEmpty(eventId),
        timestamp = getOrEmpty(eventTimestamp),
        site = getOrEmpty(eventSite),
        siteLanguage = getOrEmpty(eventSiteLanguage),
        userId = getOrEmpty(userId),
        apiKey = getOrEmpty(apiKey),
        userSessionId = getOrEmpty(userSessionId),
        transactionDuration = {
          // as the duration can not be negative I enforce it here:
          val d = FieldReader.read[Long](transactionDuration)
          if (d < 0) 1 else d
        },
        isResultCached = FieldReader.read[Boolean](isResultCached),
        referrer = getOrEmpty(eventReferrer),
        pageName = getOrEmpty(pageName),
        requestUri = getOrEmpty(requestUri)).map { baseInfo =>
          new EventDetail(
            basicInfo = baseInfo,
            userInfo = User(
              ip = getOrEmpty(userIP), agent = getOrEmpty(userAgent),
              isRobot = FieldReader.read[Boolean](userIsRobot),
              location = getOrEmpty(userLocation), browser = getOrEmpty(userBrowser)),
            searchInfo = new SearchInfo(
              id = getOrEmpty(searchId), what = getOrEmpty(searchWhat), where = getOrEmpty(searchWhere), resultCount = getOrEmpty(searchResultCount),
              whatResolved = getOrEmpty(searchWhatResolved),
              isDisambiguation = FieldReader.read[Boolean](searchIsDisambiguation),
              isSuggestion = FieldReader.read[Boolean](searchIsSuggestion),
              successful = FieldReader.read[Boolean](searchFailedOrSuccess),
              hasRHSListings = FieldReader.read[Boolean](searchHasRHSListings),
              hasNonAdRollupListings = FieldReader.read[Boolean](searchHasNonAdRollupListings),
              calledBing = FieldReader.read[Boolean](searchIsCalledBing),
              searchGeoOrDir = getOrEmpty(searchGeoOrDir), categoryId = getOrEmpty(categoryId),
              tierId = getOrEmpty(tierId),
              tierCount = FieldReader.read[Long](tierCount),
              geoName = getOrEmpty(searchGeoName), geoType = getOrEmpty(searchGeoType),
              geoPolygonIds = getOrEmpty(searchGeoPolygonIds), tierUdacCountList = getOrEmpty(tierUdacCountList),
              directoryId = -1L, headingId = -1L, headingRelevance = "",
              searchType = getOrEmpty(searchType),
              resultPage = FieldReader.read[Int](searchResultPage),
              resultPerPage = FieldReader.read[Int](searchResultPerPage),
              searchLatitude = FieldReader.read[Double](searchLatitude),
              searchLongitude = FieldReader.read[Double](searchLongitude)),
            merchantInfo = MerchantsInfo(
              id = getOrEmpty(merchantId),
              zone = getOrEmpty(merchantZone),
              latitude = getOrEmpty(merchantLatitude),
              longitude = getOrEmpty(merchantLongitude),
              distance = getOrEmpty(merchantDistance),
              displayPosition = getOrEmpty(merchantDisplayPosition),
              isNonAdRollup = getOrEmpty(merchantIsNonAdRollup),
              rank = getOrEmpty(merchantRank),
              isRelevantListing = getOrEmpty(merchantIsRelevantListing),
              isRelevantHeading = getOrEmpty(merchantIsRelevantHeading),
              headingIdList = getOrEmpty(merchantHeadingIdList),
              channel1List = getOrEmpty(merchantChannel1List),
              channel2List = getOrEmpty(merchantChannel2List),
              productType = getOrEmpty(productType),
              productLanguage = getOrEmpty(productLanguage),
              productUdac = getOrEmpty(productUdac),
              listingType = getOrEmpty(merchantListingType)),
            searchAnalysis = SearchAnalysis(
              isFuzzy = FieldReader.read[Boolean](searchAnalysisIsfuzzy),
              isGeoExpanded = FieldReader.read[Boolean](searchAnalysisIsGeoExpanded),
              isByBusinessName = FieldReader.read[Boolean](searchAnalysisIsBusinessName)),
            /* ******************************************** */
            key = getOrEmpty(key),
            value = getOrEmpty(value))
        }.
        map { anEventDetail =>
          EventDetail.sanityCheck(anEventDetail) match {
            case None => (None, Set.empty, Set.empty)
            case Some(ed) => {
              (Some(ed),
                headingsWithCats.map { case (headId, cat) => EventsHeadingsAssociation(eventId = ed.basicInfo.id, headingId = headId, category = cat) },
                dirIds.map { dirId => EventsDirectoriesAssociation(eventId = ed.basicInfo.id, directoryId = dirId) })
            }
          }
        }.getOrElse(None, Set.empty, Set.empty)
    }
  }

}

