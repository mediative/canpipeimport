package canpipe.xml

import util.xml.{ Field => XMLField }

object XMLFields {
  /* ******************************************** */
  /* Main event attributes and fields */
  val eventId = XMLField(s"Event/@id")
  val eventTimestamp = XMLField("Event/@timestamp")
  val eventSite = XMLField("Event/@timestamp")
  val eventSiteLanguage = XMLField("Event/@siteLanguage")
  val eventType = XMLField("Event/@eventType")
  val userId = XMLField("Event/@userId")
  val apiKey = XMLField("Event/apiKey")
  val userSessionId = XMLField("Event/sessionId")
  val transactionDuration = XMLField("Event/transactionDuration")
  val isResultCached = XMLField("Event/cachingUsed")
  val eventReferrer = XMLField("Event/referrer")
  val pageName = XMLField("Event/pageName")
  val requestUri = XMLField("Event/requestUri")
  /* ******************************************** */
  /* User attributes and fields */
  val userIP = XMLField("Event/user/ip")
  val userAgent = XMLField("Event/user/userAgent")
  val userIsRobot = XMLField("Event/user/robot")
  val userLocation = XMLField("Event/user/location")
  val userBrowser = XMLField("Event/user/browser")
  /* ******************************************** */
  /* Search attributes and fields */
  val searchId = XMLField("Event/search/searchId")
  val searchWhat = XMLField("Event/search/what")
  val searchWhere = XMLField("Event/search/where")
  val searchResultCount = XMLField("Event/search/resultCount")
  val searchWhatResolved = XMLField("Event/search/resolvedWhat")
  val searchIsDisambiguation = XMLField("Event/search/disambiguationPopup")
  val searchIsSuggestion = XMLField("Event/search/dYMSuggestions")
  val searchFailedOrSuccess = XMLField("Event/search/failedOrSuccess")
  val searchHasRHSListings = XMLField("Event/search/hasRHSListings")
  val searchHasNonAdRollupListings = XMLField("Event/search/hasNonAdRollupListings")
  val searchIsCalledBing = XMLField("Event/search/calledBing")
  val searchGeoOrDir = XMLField("Event/search/geoORdir")
  val categoryId = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id")
  val tierId = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id")
  val tierCount = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count")
  val searchGeoName = XMLField("Event/search/matchedGeo/geo")
  val searchGeoType = XMLField("Event/search/matchedGeo/type")
  val searchGeoPolygonIds = XMLField("Event/search/matchedGeo/polygonIds")
  val tierUdacCountList = XMLField("Event/search/allListingsTypesMainLists")
  val directoriesReturned = XMLField("Event/search/directoriesReturned")
  val headingId = XMLField("Event/search/allHeadings/heading/name")
  val headingRelevance = XMLField("Event/search/allHeadings/heading/category")
  val searchType = XMLField("Event/search/type")
  val searchResultPage = XMLField("Event/search/resultPage")
  val searchResultPerPage = XMLField("Event/search/resultPerPage")
  val searchLatitude = XMLField("Event/search/latitude")
  val searchLongitude = XMLField("Event/search/longitude")
  /* ******************************************** */
  /* Merchants attributes and fields */
  val merchantId = XMLField("Event/search/merchants/@id")
  val merchantZone = XMLField("Event/search/merchants/@zone")
  val merchantLatitude = XMLField("Event/search/merchants/@latitude")
  val merchantLongitude = XMLField("Event/search/merchants/@longitude")
  val merchantDistance = XMLField("Event/search/merchants/@distance")
  val merchantDisplayPosition = XMLField("Event/search/merchants/RHSorLHS")
  val merchantIsNonAdRollup = XMLField("Event/search/merchants/isNonAdRollup")
  val merchantRank = XMLField("Event/search/merchants/ranking")
  val merchantIsRelevantListing = XMLField("Event/search/merchants/isListingRelevant")
  val merchantIsRelevantHeading = XMLField("Event/search/merchants/entry/heading/@isRelevant")
  val merchantHeadingIdList = XMLField("Event/search/merchants/entry/heading/categories")
  val merchantChannel1List = XMLField("Event/search/merchants/entry/directories/channel1")
  val merchantChannel2List = XMLField("Event/search/merchants/entry/directories/channel2")
  val productType = XMLField("Event/search/merchants/entry/product/productType")
  val productLanguage = XMLField("Event/search/merchants/entry/product/language")
  val productUdac = XMLField("Event/search/merchants/entry/product/udac")
  val merchantListingType = XMLField("Event/search/merchants/entry/listingType")
  /* ******************************************** */
  /* Search Analytics/Analysis attributes and fields */
  val searchAnalysisIsfuzzy = XMLField("Event/search/searchAnalysis/fuzzy")
  val searchAnalysisIsGeoExpanded = XMLField("Event/search/searchAnalysis/geoExpanded")
  val searchAnalysisIsBusinessName = XMLField("Event/search/searchAnalysis/businessName")
  val key = XMLField("Event/searchAnalytics/entry/@key")
  val value = XMLField("Event/searchAnalytics/entry/@value")
}
