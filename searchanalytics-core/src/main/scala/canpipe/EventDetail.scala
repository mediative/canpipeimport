package canpipe

import canpipe.xml.XMLFields

import scala.util.control.Exception._
import XMLFields._
import util.xml.{ Field => XMLField }
import canpipe.xml.{ Elem => CanpipeXMLElem }
import util.xml.Base._ // among other things, brings implicits into scope

class EventDetail(
  /* ******************************************** */
  /* Main event attributes and fields */
  val eventId: String, /* /root/Event/@id */
  val eventTimestamp: String, /* /root/Event/@timestamp */
  val timestampId: Long, /* FK to Time table */
  val eventSite: String, /* /root/Event/@site */
  val eventSiteLanguage: String, /* /root/Event/@siteLanguage */
  val eventType: String,
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
         | eventType = ${eventType},
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

  override def productArity: Int = 65

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int) = n match {
    case 0 => eventId
    case 1 => eventTimestamp
    case 2 => timestampId
    case 3 => eventSite
    case 4 => eventSiteLanguage
    case 5 => eventType
    case 6 => userId
    case 7 => apiKey
    case 8 => userSessionId
    case 9 => transactionDuration
    case 10 => isResultCached
    case 11 => eventReferrer
    case 12 => pageName
    case 13 => requestUri
    case 14 => userIP
    case 15 => userAgent
    case 16 => userIsRobot
    case 17 => userLocation
    case 18 => userBrowser
    case 19 => searchId
    case 20 => searchWhat
    case 21 => searchWhere
    case 22 => searchResultCount
    case 23 => searchWhatResolved
    case 24 => searchIsDisambiguation
    case 25 => searchIsSuggestion
    case 26 => searchFailedOrSuccess
    case 27 => searchHasRHSListings
    case 28 => searchHasNonAdRollupListings
    case 29 => searchIsCalledBing
    case 30 => searchGeoOrDir
    case 31 => categoryId
    case 32 => tierId
    case 33 => tierCount
    case 34 => searchGeoName
    case 35 => searchGeoType
    case 36 => searchGeoPolygonIds
    case 37 => tierUdacCountList
    case 38 => searchType
    case 39 => searchResultPage
    case 40 => searchResultPerPage
    case 41 => searchLatitude
    case 42 => searchLongitude
    case 43 => merchantId
    case 44 => merchantZone
    case 45 => merchantLatitude
    case 46 => merchantLongitude
    case 47 => merchantDistance
    case 48 => merchantDisplayPosition
    case 49 => merchantIsNonAdRollup
    case 50 => merchantRank
    case 51 => merchantIsRelevantListing
    case 52 => merchantIsRelevantHeading
    case 53 => merchantHeadingIdList
    case 54 => merchantChannel1List
    case 55 => merchantChannel2List
    case 56 => productType
    case 57 => productLanguage
    case 58 => productUdac
    case 59 => merchantListingType
    case 60 => searchAnalysisIsfuzzy
    case 61 => searchAnalysisIsGeoExpanded
    case 62 => searchAnalysisIsBusinessName
    case 63 => key
    case 64 => value
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

}

object EventDetail {

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

}

