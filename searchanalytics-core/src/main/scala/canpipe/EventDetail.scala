package canpipe

import util.errorhandler.Base.GenericHandler
import util.types.reader.Base.ReadSym
import util.xml.Base._
import canpipe.xml.XMLFields
import util.Logging
import scala.util.control.Exception._
import XMLFields._
import util.xml.{ Field => XMLField }
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.reader.{ Base => Readers }
import Readers._

case class EventDetail(
  basicInfo: EventDetail.BasicInfo,
  userInfo: EventDetail.User,
  searchInfo: EventDetail.SearchInfo,
  merchantInfo: EventDetail.MerchantsInfo,
  searchAnalysis: EventDetail.SearchAnalysis,
  /* ******************************************** */
  key: String,
  value: String) extends Serializable {

  override def toString = {
    s"""
         | basic info = ${basicInfo.toString},
         |  user info = ${userInfo.toString},
         |  search info = ${searchInfo.toString},
         | merchant info = ${merchantInfo.toString},
         | search Analysis Info = ${searchAnalysis.toString},
         | key (/root/Event/searchAnalytics/entry/@key) = ${key},
         | value (/root/Event/searchAnalytics/entry/@value) = ${value}
         """.stripMargin
  }

}

object EventDetail extends Logging {

  object Language extends Enumeration {
    type Language = Value
    val EN, FR = Value
    def apply(aLangAsString: String): Option[Language] = {
      (aLangAsString.trim.toUpperCase.substring(0, 2) match {
        case "FR" => Some(FR)
        case "EN" => Some(EN)
        case _ =>
          logger.error(s"Language '${aLangAsString}' is not recognized")
          None
      })
    }
  }
  import Language._

  /**
   * Id used for timestamps in the current analytics tables (CAA's)
   */
  trait AnalyticsTimestampId {
    val value: Long
  }

  object AnalyticsTimestampId {
    val DateFromXMLRegex = """(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).(\d\d\d)-(\d\d):(\d\d)""".r

    def apply(theTimestamp: String): Option[AnalyticsTimestampId] = {
      catching(classOf[Exception]).opt {
        val DateFromXMLRegex(year, month, day, hour, mins, secs, msecs, hourToGMT, minsToGMT) = theTimestamp
        year.toLong * 10000 + month.toLong * 100 + day.toLong
      }.map(new AnalyticsTimestampImpl(_))
    }
    def apply(theTimestamp: Long): Option[AnalyticsTimestampId] = {
      val a = theTimestamp.toString.map(_.asDigit).toArray
      if (a.length != 8) None
      else {
        val year = a(0) * 1000 + a(1) * 100 + a(2) * 10 + a(3)
        val month = a(4) * 10 + a(5)
        val day = a(6) * 10 + a(7)
        if ((year >= 1900) && (year <= 2050) && (month >= 1) && (month <= 12) && (day >= 1) && (day <= 30)) Some(AnalyticsTimestampImpl(theTimestamp))
        else None
      }
    }
    case class AnalyticsTimestampImpl(value: Long) extends AnalyticsTimestampId
  }

  /* Main event attributes and fields */
  // TODO: we need "/root/Event/@eventType": String, // Two values are possible "impression" which is a SERP event, or "click" which is an MP event
  trait BasicInfo extends Serializable {
    val id: String
    val timestamp: String
    val timestampId: AnalyticsTimestampId
    val site: String
    val siteLanguage: Language
    val userId: String
    val apiKey: String
    val userSessionId: String
    val transactionDuration: Long
    val isResultCached: Boolean
    val referrer: String
    val pageName: String
    val requestUri: String
  }

  object BasicInfo {
    private def build(id: String, timestamp: String, timestampIdOpt: Option[AnalyticsTimestampId], site: String,
                      siteLanguageOpt: Option[Language],
                      userId: String, apiKey: String, userSessionId: String, transactionDuration: Long,
                      isResultCached: Boolean, referrer: String, pageName: String, requestUri: String): Option[BasicInfo] = {
      timestampIdOpt.flatMap { tId =>
        siteLanguageOpt.map { lang =>
          new BasicInfoImpl(
            id, timestamp, tId, site, lang, userId, apiKey, userSessionId, transactionDuration, isResultCached, referrer, pageName, requestUri)
        }
      }
    }
    def apply(id: String, timestamp: String, site: String, siteLanguage: String,
              userId: String, apiKey: String, userSessionId: String, transactionDuration: Long,
              isResultCached: Boolean, referrer: String, pageName: String, requestUri: String): Option[BasicInfo] = {
      build(id, timestamp, timestampIdOpt = AnalyticsTimestampId(timestamp), site,
        siteLanguageOpt = Language(siteLanguage),
        userId, apiKey, userSessionId, transactionDuration,
        isResultCached, referrer, pageName, requestUri)
    }
    def apply(id: String, timestamp: String, site: String, siteLanguage: Language,
              userId: String, apiKey: String, userSessionId: String, transactionDuration: Long,
              isResultCached: Boolean, referrer: String, pageName: String, requestUri: String): Option[BasicInfo] = {
      build(id, timestamp, timestampIdOpt = AnalyticsTimestampId(timestamp), site,
        siteLanguageOpt = Some(siteLanguage),
        userId, apiKey, userSessionId, transactionDuration,
        isResultCached, referrer, pageName, requestUri)
    }
    def apply(id: String, timestamp: String, timestampId: AnalyticsTimestampId, site: String, siteLanguage: String,
              userId: String, apiKey: String, userSessionId: String, transactionDuration: Long,
              isResultCached: Boolean, referrer: String, pageName: String, requestUri: String): Option[BasicInfo] = {
      build(id, timestamp, timestampIdOpt = Some(timestampId), site,
        siteLanguageOpt = Language(siteLanguage),
        userId, apiKey, userSessionId, transactionDuration,
        isResultCached, referrer, pageName, requestUri)
    }
    def apply(id: String, timestamp: String, timestampId: AnalyticsTimestampId, site: String, siteLanguage: Language,
              userId: String, apiKey: String, userSessionId: String, transactionDuration: Long,
              isResultCached: Boolean, referrer: String, pageName: String, requestUri: String): BasicInfo = {
      new BasicInfoImpl(id, timestamp, timestampId, site, siteLanguage, userId, apiKey, userSessionId, transactionDuration, isResultCached, referrer, pageName, requestUri)
    }

    private case class BasicInfoImpl(
      id: String, timestamp: String, timestampId: AnalyticsTimestampId, site: String, siteLanguage: Language, userId: String,
      apiKey: String, userSessionId: String, transactionDuration: Long, isResultCached: Boolean, referrer: String,
      pageName: String, requestUri: String) extends BasicInfo
  }

  /* User attributes and fields */
  case class User(ip: String, agent: String, isRobot: Boolean, location: String, browser: String) extends Serializable

  /* Search attributes and fields */
  class SearchInfo(
    val id: String, val what: String, val where: String, val resultCount: String, val whatResolved: String,
    val isDisambiguation: Boolean, val isSuggestion: Boolean, val successful: Boolean, val hasRHSListings: Boolean,
    val hasNonAdRollupListings: Boolean, val calledBing: Boolean,
    // FROM https://wiki.ypg.com/pages/viewpage.action?pageId=165612529: Values are "geo" or "dir" on a SERP. Field should be empty on a merchant page
    val searchGeoOrDir: String,
    val categoryId: String, val tierId: String, val tierCount: Long,
    val geoName: String, val geoType: String, val geoPolygonIds: String,
    val tierUdacCountList: String,
    val directoryId: Long, val headingId: Long,
    val headingRelevance: String /* 'A' or 'B'*/ , // TODO: put this as Char. Spark had problems with it - sove them! scala.MatchError: scala.Char (of class scala.reflect.internal.Types$TypeRef$$anon$6)
    val searchType: String, // TODO: "The search sort type (si, si-pop, si-rat, si-rev, si-alph)" (https://wiki.ypg.com/pages/viewpage.action?pageId=165612529)
    val resultPage: Int, val resultPerPage: Int,
    val searchLatitude: Double, val searchLongitude: Double) extends Serializable {
    def copy(
      searchId: String = id, searchWhat: String = what, searchWhere: String = where, searchResultCount: String = resultCount,
      searchWhatResolved: String = whatResolved, searchIsDisambiguation: Boolean = isDisambiguation,
      searchIsSuggestion: Boolean = isSuggestion, searchFailedOrSuccess: Boolean = successful,
      searchHasRHSListings: Boolean = hasRHSListings, searchHasNonAdRollupListings: Boolean = hasNonAdRollupListings,
      searchIsCalledBing: Boolean = calledBing, searchGeoOrDir: String = searchGeoOrDir,
      categoryId: String = categoryId, tierId: String = tierId, tierCount: Long = tierCount,
      searchGeoName: String = geoName, searchGeoType: String = geoType, searchGeoPolygonIds: String = geoPolygonIds,
      tierUdacCountList: String = tierUdacCountList, directoryId: Long = directoryId,
      headingId: Long = headingId, headingRelevance: String = headingRelevance,
      searchType: String = searchType, searchResultPage: Int = resultPage, searchResultPerPage: Int = resultPerPage,
      searchLatitude: Double = searchLatitude, searchLongitude: Double = searchLongitude): SearchInfo = {
      new SearchInfo(searchId, searchWhat, searchWhere, searchResultCount, searchWhatResolved, searchIsDisambiguation,
        searchIsSuggestion, searchFailedOrSuccess, searchHasRHSListings, searchHasNonAdRollupListings,
        searchIsCalledBing, searchGeoOrDir, categoryId, tierId, tierCount,
        searchGeoName, searchGeoType, searchGeoPolygonIds,
        tierUdacCountList,
        directoryId, headingId, headingRelevance,
        searchType, searchResultPage, searchResultPerPage,
        searchLatitude, searchLongitude)
    }
  }

  /* Merchants attributes and fields */
  case class MerchantsInfo(
    id: String,
    zone: String,
    latitude: String,
    longitude: String,
    distance: String, // TODO: when merchants are de-normalized, this field should be Long
    displayPosition: String,
    isNonAdRollup: String, // TODO: when merchants are de-normalized, this field should be Boolean
    rank: String, // TODO: when merchants are de-normalized, this field should be Int
    isRelevantListing: String, // TODO: when merchants are de-normalized, this field should be Boolean
    isRelevantHeading: String, // TODO: when merchants are de-normalized, this field should be Boolean
    headingIdList: String,
    channel1List: String,
    channel2List: String,
    productType: String,
    productLanguage: String,
    productUdac: String,
    listingType: String) extends Serializable

  /* Search Analytics/Analysis attributes and fields */
  case class SearchAnalysis(
    isFuzzy: Boolean,
    isGeoExpanded: Boolean,
    isByBusinessName: Boolean) extends Serializable

  /**
   * TODO
   * @param aMap
   * @return
   */
  def apply(anXMLNode: CanpipeXMLElem): Seq[EventDetail] = {
    def getOrEmpty(anXMLField: XMLField): String = {
      (anXMLNode.value \ anXMLField.asList).mkString(",")
    }

    // Readers helpers:
    object FieldReader extends Logging {
      import util.errorhandler.Base.{ LongHandler, IntHandler, DoubleHandler, BooleanHandler }
      import util.types.reader.Base.{ LongReader, IntReader, DoubleReader, BooleanReader }
      def read[T](field: util.xml.Field)(implicit theReader: ReadSym[T], errHandler: GenericHandler[T]): T = {
        val fieldValue = getOrEmpty(field)
        errHandler.handle(theReader.read(fieldValue)).left.map { aMsg => s"${field.asString} incorrectly set to '${fieldValue}': ${aMsg}" } match {
          case Left(errMsg) =>
            logger.error(errMsg); theReader.default
          case Right(v) => v
        }
      }
    }

    def fillEventDetailWithPrototype(firstEd: EventDetail,
                                     aDirectoryId: Long,
                                     aHeading: Long,
                                     itsCategory: String): EventDetail = {
      firstEd.copy(basicInfo = BasicInfo(
        id = firstEd.basicInfo.id,
        site = firstEd.basicInfo.site,
        siteLanguage = firstEd.basicInfo.siteLanguage,
        userId = firstEd.basicInfo.userId,
        apiKey = firstEd.basicInfo.apiKey,
        userSessionId = firstEd.basicInfo.userSessionId,
        transactionDuration = firstEd.basicInfo.transactionDuration,
        isResultCached = firstEd.basicInfo.isResultCached,
        referrer = firstEd.basicInfo.referrer,
        pageName = firstEd.basicInfo.pageName,
        requestUri = firstEd.basicInfo.requestUri,
        timestamp = firstEd.basicInfo.timestamp,
        timestampId = firstEd.basicInfo.timestampId))
    }

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
    val (headingsIds, headingsCats): (Seq[Long], Seq[String]) =
      (anXMLNode.value \ headingId.asList) match {
        case l if l.isEmpty => (List(-1), List("")) // TODO: if no heading, spit -1L. That OK?
        case _ => ((anXMLNode.value \ headingId.asList).map(_.toLong), (anXMLNode.value \ headingRelevance.asList))
      }
    val headingsWithCats = (headingsIds zip headingsCats).toSet
    val dirsHeadingsAndCats = for (dir <- dirIds; headingAndCat <- headingsWithCats) yield (dir, headingAndCat)

    BasicInfo(
      id = getOrEmpty(eventId),
      timestamp = getOrEmpty(eventTimestamp),
      site = getOrEmpty(eventSite),
      siteLanguage = getOrEmpty(eventSiteLanguage),
      userId = getOrEmpty(userId),
      apiKey = getOrEmpty(apiKey),
      userSessionId = getOrEmpty(userSessionId),
      transactionDuration = FieldReader.read[Long](transactionDuration),
      isResultCached = FieldReader.read[Boolean](isResultCached),
      referrer = getOrEmpty(eventReferrer),
      pageName = getOrEmpty(pageName),
      requestUri = getOrEmpty(requestUri)).map { baseInfo =>
        val firstEd = new EventDetail(
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
        dirsHeadingsAndCats.foldLeft(Seq.empty[EventDetail]) {
          case (listOfEventOpts, (aDirectoryId, (aHeading, itsCategory))) =>
            fillEventDetailWithPrototype(firstEd, aDirectoryId, aHeading, itsCategory) +: listOfEventOpts
        }
      }.getOrElse(Seq.empty)

  }

}

