package canpipe.parser.spark

import canpipe.xml.{ Elem => CanpipeXMLElem }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.util.{ Base => SparkUtil }
import util.Logging
import canpipe.{ EventDetail => BasicEventDetail, Tables }
import util.wrapper.Wrapper

// Class used to save data as Parquet. Contains only primitive types, as guided by
// http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_datatypes.html
// Specifically: "Currently, Impala supports only scalar types, not composite or nested types. Accessing a table containing any columns with unsupported types causes an error."
private[spark] class EventDetailPrivate(
  /* ******************************************** */
  /* Main event attributes and fields */
  val eventId: String,
  val eventTimestamp: String,
  val timestampId: Long,
  val eventSite: String,
  val eventSiteLanguage: String,
  val userId: String,
  val apiKey: String, val userSessionId: String,
  val transactionDuration: Long,
  val isResultCached: Boolean,
  val eventReferrer: String,
  val pageName: String,
  val requestUri: String,
  /* ******************************************** */
  /* User attributes and fields */
  val userIP: String,
  val userAgent: String,
  val userIsRobot: Boolean,
  val userLocation: String,
  val userBrowser: String,
  /* ******************************************** */
  /* Search attributes and fields */
  val searchId: String,
  val searchWhat: String, val searchWhere: String,
  val searchResultCount: String,
  val searchWhatResolved: String,
  val searchIsDisambiguation: Boolean,
  val searchIsSuggestion: Boolean,
  val searchFailedOrSuccess: String,
  val searchHasRHSListings: Boolean,
  val searchHasNonAdRollupListings: Boolean,
  val searchIsCalledBing: Boolean, val searchGeoOrDir: String,
  val categoryId: String,
  val tierId: String,
  val tierCount: Long,
  val searchGeoName: String,
  val searchGeoType: String,
  val searchGeoPolygonIds: String,
  val tierUdacCountList: String,
  val directoryId: Long,
  val headingId: Long,
  val headingRelevance: String /* 'A' or 'B'*/ , // TODO: put this as Char. Spark had problems with it - sove them! scala.MatchError: scala.Char (of class scala.reflect.internal.Types$TypeRef$$anon$6)
  val searchType: String,
  val searchResultPage: Int,
  val searchResultPerPage: Int,
  val searchLatitude: Double,
  val searchLongitude: Double,
  /* ******************************************** */
  /* Merchants attributes and fields */
  val merchantId: String,
  val merchantZone: String,
  val merchantLatitude: String,
  val merchantLongitude: String,
  val merchantDistance: String, // TODO: when merchants are de-normalized, this field should be Long
  val merchantDisplayPosition: String,
  val merchantIsNonAdRollup: String, // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantRank: String, // TODO: when merchants are de-normalized, this field should be Int
  val merchantIsRelevantListing: String, // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantIsRelevantHeading: String, // TODO: when merchants are de-normalized, this field should be Boolean
  val merchantHeadingIdList: String,
  val merchantChannel1List: String,
  val merchantChannel2List: String,
  val productType: String,
  val productLanguage: String,
  val productUdac: String,
  val merchantListingType: String,
  /* ******************************************** */
  /* Search Analytics/Analysis attributes and fields */
  val searchAnalysisIsfuzzy: Boolean,
  val searchAnalysisIsGeoExpanded: Boolean,
  val searchAnalysisIsBusinessName: Boolean,
  val key: String,
  val value: String)
  extends Product with Serializable {

  override def toString = {
    s"""
         | eventId = ${eventId}, eventTimestamp = ${eventTimestamp}, eventSite = ${eventSite}, eventSiteLanguage = ${eventSiteLanguage},
         | userId = ${userId}, apiKey = ${apiKey}, userSessionId = ${userSessionId}, transactionDuration = ${transactionDuration},
         | isResultCached = ${isResultCached}, eventReferrer = ${eventReferrer}, pageName = ${pageName}, requestUri = ${requestUri},
         |  userIP = ${userIP}, userAgent = ${userAgent}, userIsRobot = ${userIsRobot}, userLocation = ${userLocation},
         |  userBrowser = ${userBrowser}, searchId = ${searchId}, searchWhat = ${searchWhat}, searchWhere = ${searchWhere},
         |  searchResultCount = ${searchResultCount}, searchWhatResolved = ${searchWhatResolved}, searchIsDisambiguation = ${searchIsDisambiguation},
         |  searchIsSuggestion = ${searchIsSuggestion}, searchFailedOrSuccess = ${searchFailedOrSuccess},
         |  searchHasRHSListings = ${searchHasRHSListings}, searchHasNonAdRollupListings = ${searchHasNonAdRollupListings},
         |  searchIsCalledBing = ${searchIsCalledBing}, searchGeoOrDir = ${searchGeoOrDir}, categoryId = ${categoryId},
         |  tierId = ${tierId}, tierCount = ${tierCount}, searchGeoName = ${searchGeoName}, searchGeoType = ${searchGeoType},
         | searchGeoPolygonIds = ${searchGeoPolygonIds}, tierUdacCountList = ${tierUdacCountList}, directoryId = ${directoryId},
         | headingId = ${headingId}, headingRelevance = ${headingRelevance}, searchType = ${searchType},
         | searchResultPage = ${searchResultPage}, searchResultPerPage = ${searchResultPerPage}, searchLatitude = ${searchLatitude},
         | searchLongitude = ${searchLongitude}, merchantId = ${merchantId}, merchantZone = ${merchantZone},
         | merchantLatitude = ${merchantLatitude}, merchantLongitude = ${merchantLongitude}, merchantDistance = ${merchantDistance},
         | merchantDisplayPosition = ${merchantDisplayPosition}, merchantIsNonAdRollup = ${merchantIsNonAdRollup},
         | merchantRank = ${merchantRank}, merchantIsRelevantListing = ${merchantIsRelevantListing},
         | merchantIsRelevantHeading = ${merchantIsRelevantHeading}, merchantHeadingIdList = ${merchantHeadingIdList},
         | merchantChannel1List = ${merchantChannel1List}, merchantChannel2List = ${merchantChannel2List},
         | productType = ${productType}, productLanguage = ${productLanguage}, productUdac = ${productUdac},
         | merchantListingType = ${merchantListingType}, searchAnalysisIsfuzzy = ${searchAnalysisIsfuzzy},
         | searchAnalysisIsGeoExpanded = ${searchAnalysisIsGeoExpanded}, searchAnalysisIsBusinessName = ${searchAnalysisIsBusinessName},
         | key = ${key}, value = ${value}
         """.stripMargin
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EventDetailPrivate]

  override def hashCode = eventId.toUpperCase.hashCode

  override def equals(o: Any) = o match {
    case that: EventDetailPrivate =>
      (0 to productArity - 1).map(i => this.productElement(i) == that.productElement(i)).forall(_ == true)
    case _ => false
  }

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

/**
 * Spark's Event Detail is just a wrapper around the 'real' details.
 */
case class EventDetail(value: BasicEventDetail) extends Wrapper[BasicEventDetail]

object EventDetail extends Logging {

  def apply(anXMLNode: CanpipeXMLElem): Option[EventDetail] = {
    Tables(anXMLNode).eventOpt.map(new EventDetail(_))
  }

  def saveAsParquet(sc: SparkContext, parquetFileName: String, c: RDD[EventDetail], force: Boolean = false): Boolean = {
    val rddEdPrivate =
      c.map { anEventDetail =>
        val basicED = anEventDetail.value
        new EventDetailPrivate(
          eventId = basicED.basicInfo.id,
          eventTimestamp = basicED.basicInfo.timestamp,
          timestampId = basicED.basicInfo.timestampId.value,
          eventSite = basicED.basicInfo.site,
          eventSiteLanguage = basicED.basicInfo.siteLanguage.toString,
          userId = basicED.basicInfo.userId,
          apiKey = basicED.basicInfo.apiKey,
          userSessionId = basicED.basicInfo.userSessionId,
          transactionDuration = basicED.basicInfo.transactionDuration,
          isResultCached = basicED.basicInfo.isResultCached,
          eventReferrer = basicED.basicInfo.referrer,
          pageName = basicED.basicInfo.pageName,
          requestUri = basicED.basicInfo.requestUri,
          /* ******************************************** */
          userIP = basicED.userInfo.ip,
          userAgent = basicED.userInfo.agent,
          userIsRobot = basicED.userInfo.isRobot,
          userLocation = basicED.userInfo.location,
          userBrowser = basicED.userInfo.browser,
          /* ******************************************** */
          searchId = basicED.searchInfo.id,
          searchWhat = basicED.searchInfo.what, searchWhere = basicED.searchInfo.where,
          searchResultCount = basicED.searchInfo.resultCount,
          searchWhatResolved = basicED.searchInfo.whatResolved,
          searchIsDisambiguation = basicED.searchInfo.isDisambiguation,
          searchIsSuggestion = basicED.searchInfo.isSuggestion,
          searchFailedOrSuccess = if (basicED.searchInfo.successful) "success" else "failed", // to be able to conform to https://wiki.ypg.com/pages/viewpage.action?pageId=165612529
          searchHasRHSListings = basicED.searchInfo.hasRHSListings,
          searchHasNonAdRollupListings = basicED.searchInfo.hasNonAdRollupListings,
          searchIsCalledBing = basicED.searchInfo.calledBing, searchGeoOrDir = basicED.searchInfo.searchGeoOrDir,
          categoryId = basicED.searchInfo.categoryId,
          tierId = basicED.searchInfo.tierId,
          tierCount = basicED.searchInfo.tierCount,
          searchGeoName = basicED.searchInfo.geoName,
          searchGeoType = basicED.searchInfo.geoType,
          searchGeoPolygonIds = basicED.searchInfo.geoPolygonIds,
          tierUdacCountList = basicED.searchInfo.tierUdacCountList,
          directoryId = basicED.searchInfo.directoryId,
          headingId = basicED.searchInfo.headingId,
          headingRelevance = basicED.searchInfo.headingRelevance,
          searchType = basicED.searchInfo.searchType,
          searchResultPage = basicED.searchInfo.resultPage,
          searchResultPerPage = basicED.searchInfo.resultPerPage,
          searchLatitude = basicED.searchInfo.searchLatitude,
          searchLongitude = basicED.searchInfo.searchLongitude,
          /* ******************************************** */
          merchantId = basicED.merchantInfo.id,
          merchantZone = basicED.merchantInfo.zone,
          merchantLatitude = basicED.merchantInfo.latitude,
          merchantLongitude = basicED.merchantInfo.longitude,
          merchantDistance = basicED.merchantInfo.distance,
          merchantDisplayPosition = basicED.merchantInfo.displayPosition,
          merchantIsNonAdRollup = basicED.merchantInfo.isNonAdRollup,
          merchantRank = basicED.merchantInfo.rank,
          merchantIsRelevantListing = basicED.merchantInfo.isRelevantListing,
          merchantIsRelevantHeading = basicED.merchantInfo.isRelevantHeading,
          merchantHeadingIdList = basicED.merchantInfo.headingIdList,
          merchantChannel1List = basicED.merchantInfo.channel1List,
          merchantChannel2List = basicED.merchantInfo.channel2List,
          productType = basicED.merchantInfo.productType,
          productLanguage = basicED.merchantInfo.productLanguage,
          productUdac = basicED.merchantInfo.productUdac,
          merchantListingType = basicED.merchantInfo.listingType,
          /* ******************************************** */
          /* Search Analytics/Analysis attributes and fields */
          searchAnalysisIsfuzzy = basicED.searchAnalysis.isFuzzy,
          searchAnalysisIsGeoExpanded = basicED.searchAnalysis.isGeoExpanded,
          searchAnalysisIsBusinessName = basicED.searchAnalysis.isByBusinessName,
          key = basicED.key,
          value = basicED.value)
      }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    if (!force && SparkUtil.HDFS.fileExists(parquetFileName)) {
      logger.error(s"File '${parquetFileName}' already exists")
      false
    } else {
      rddEdPrivate.saveAsParquetFile(parquetFileName)
      true
    }
  }

}

