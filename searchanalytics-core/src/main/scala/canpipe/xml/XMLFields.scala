package canpipe.xml

import util.xml.{ Field => XMLField }

// all paths definition go here
/*
  TODO: maybe put priority here
  Like this? ===>
  object FieldImportance extends Enumeration {
    type FieldImportance = Value
    val Must, Should, Could = Value
  }
  import FieldImportance._

  val fieldsDef: Map[String, FieldImportance] = Map(
    "/root/Event/@id" -> Must,
    "/root/Event/@timestamp" -> Must,
    "/root/Event/timestampId" -> Must,
    "/root/Event/@site" -> Must,
    "/root/Event/@siteLanguage" -> Must,
    "/root/Event/@eventType" -> Must, // Two values are possible "impression" which is a SERP event, or "click" which is an MP event

 */
object XMLFields {
  /* ******************************************** */
  /* Main event attributes and fields */
  val EVENT_ID = XMLField(s"Event/@id")
  val EVENT_TIMESTAMP = XMLField("Event/@timestamp")
  val EVENT_SITE = XMLField("Event/@timestamp")
  val EVENT_SITELANGUAGE = XMLField("Event/@siteLanguage")
  val EVENT_TYPE = XMLField("Event/@eventType")
  val EVENT_USER_ID = XMLField("Event/@userId")
  val EVENT_API_KEY = XMLField("Event/apiKey")
  val EVENT_USER_SESSIONID = XMLField("Event/sessionId")
  val EVENT_TRANSACTION_DURATION = XMLField("Event/transactionDuration")
  val EVENT_CACHINGUSED = XMLField("Event/cachingUsed")
  val EVENT_REFERRER = XMLField("Event/referrer")
  val EVENT_PAGENAME = XMLField("Event/pageName")
  val EVENT_REQUESTURI = XMLField("Event/requestUri")
  /* ******************************************** */
  /* User attributes and fields */
  val USER_IP = XMLField("Event/user/ip")
  val USER_AGENT = XMLField("Event/user/userAgent")
  val USER_ROBOT = XMLField("Event/user/robot")
  val USER_LOCATION = XMLField("Event/user/location")
  val USER_BROWSER = XMLField("Event/user/browser")
  /* ******************************************** */
  /* Search attributes and fields */
  val SEARCH_ID = XMLField("Event/search/searchId")
  val SEARCH_WHAT = XMLField("Event/search/what")
  val SEARCH_WHERE = XMLField("Event/search/where")
  val SEARCH_RESULTCOUNT = XMLField("Event/search/resultCount")
  val SEARCH_WHATRESOLVED = XMLField("Event/search/resolvedWhat")
  val SEARCH_DISAMBIGUATIONPOPUP = XMLField("Event/search/disambiguationPopup")
  val SEARCH_DYMSUGGESTIONS = XMLField("Event/search/dYMSuggestions")
  val SEARCH_FAILEDORSUCCESS = XMLField("Event/search/failedOrSuccess")
  val SEARCH_HASRHSLISTINGS = XMLField("Event/search/hasRHSListings")
  val SEARCH_HASNONADROLLUPLISTINGS = XMLField("Event/search/hasNonAdRollupListings")
  val SEARCH_CALLEDBING = XMLField("Event/search/calledBing")
  val SEARCH_GEOORDIR = XMLField("Event/search/geoORdir")
  val SEARCH_CATEGORYID = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id")
  val SEARCH_TIERID = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id")
  val SEARCH_TIERCOUNT = XMLField("Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count")
  val SEARCH_GEONAME = XMLField("Event/search/matchedGeo/geo")
  val SEARCH_GEOTYPE = XMLField("Event/search/matchedGeo/type")
  val SEARCH_GEOPOLYGONIDS = XMLField("Event/search/matchedGeo/polygonIds")
  val SEARCH_TIERUDACCOUNTLIST = XMLField("Event/search/allListingsTypesMainLists")
  val SEARCH_DIRECTORIESRETURNED = XMLField("Event/search/directoriesReturned")
  val SEARCH_HEADING_NAME = XMLField("Event/search/allHeadings/heading/name")
  val SEARCH_HEADINGRELEVANCE = XMLField("Event/search/allHeadings/heading/category")
  val SEARCH_TYPE = XMLField("Event/search/type")
  val SEARCH_RESULTPAGE = XMLField("Event/search/resultPage")
  val SEARCH_RESULTPERPAGE = XMLField("Event/search/resultPerPage")
  val SEARCH_LATITUDE = XMLField("Event/search/latitude")
  val SEARCH_LONGITUDE = XMLField("Event/search/longitude")
  /* ******************************************** */
  /* Merchants attributes and fields */
  val MERCHANTS_ID = XMLField("Event/search/merchants/@id")
  val MERCHANTS_ZONE = XMLField("Event/search/merchants/@zone")
  val MERCHANTS_LATITUDE = XMLField("Event/search/merchants/@latitude")
  val MERCHANTS_LONGITUDE = XMLField("Event/search/merchants/@longitude")
  val MERCHANTS_DISTANCE = XMLField("Event/search/merchants/@distance")
  val MERCHANTS_RHS_OR_LHS = XMLField("Event/search/merchants/RHSorLHS")
  val MERCHANTS_NONADROLLUP = XMLField("Event/search/merchants/isNonAdRollup")
  val MERCHANTS_RANKING = XMLField("Event/search/merchants/ranking")
  val MERCHANTS_IS_RELEVANT_LISTING = XMLField("Event/search/merchants/isListingRelevant")
  val MERCHANTS_IS_RELEVANT_HEADING = XMLField("Event/search/merchants/entry/heading/@isRelevant")
  val MERCHANTS_HEADING_CATEGORIES = XMLField("Event/search/merchants/entry/heading/categories")
  val MERCHANTS_DIRECTORIES_CHANNEL1 = XMLField("Event/search/merchants/entry/directories/channel1")
  val MERCHANTS_DIRECTORIES_CHANNEL2 = XMLField("Event/search/merchants/entry/directories/channel2")
  val MERCHANTS_PRODUCT_TYPE = XMLField("Event/search/merchants/entry/product/productType")
  val MERCHANTS_PRODUCT_LANGUAGE = XMLField("Event/search/merchants/entry/product/language")
  val MERCHANTS_PRODUCT_UDAC = XMLField("Event/search/merchants/entry/product/udac")
  val MERCHANTS_LISTING_TYPE = XMLField("Event/search/merchants/entry/listingType")
  /* ******************************************** */
  /* Search Analytics/Analysis attributes and fields */
  val SEARCHANALYSIS_ISFUZZY = XMLField("Event/search/searchAnalysis/fuzzy")
  val SEARCHANALYSIS_GEOEXPANDED = XMLField("Event/search/searchAnalysis/geoExpanded")
  val SEARCHANALYSIS_BUSINESSNAME = XMLField("Event/search/searchAnalysis/businessName")
  val SEARCHANALYTICS_KEYS = XMLField("Event/searchAnalytics/entry/@key")
  val SEARCHANALYTICS_VALUES = XMLField("Event/searchAnalytics/entry/@value")
}
