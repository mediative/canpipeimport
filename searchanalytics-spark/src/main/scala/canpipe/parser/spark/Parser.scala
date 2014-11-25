package canpipe.parser.spark

import canpipe.parser.FilterRule
import org.apache.spark.rdd.RDD
import util.Base
import util.Base.XML.XPath

import scala.io.Source
import scala.xml.pull.XMLEventReader

class eventDetail(
  /* ******************************************** */
  /* Main event attributes and fields */
  val attr_id: String, /* /root/Event/@id */
  val attr_timestamp: String, /* /root/Event/@timestamp */
  val attr_site: String, /* /root/Event/@site */
  val attr_siteLanguage: String, /* /root/Event/@siteLanguage */
  // NOT NEEDED ==> (since it is ALWAYS an Impression) "/root/Event/@eventType": String, // Two values are possible "impression" which is a SERP event, or "click" which is an MP event
  val attr_userId: String, /* /root/Event/@userId */
  val attr_apiKey: String /* /root/Event/apiKey */ , val sessionId: String /* /root/Event/sessionId */ ,
  val transactionDuration: String /* /root/Event/transactionDuration */ ,
  val cachingUsed: String /* /root/Event/cachingUsed */ , val referrer: String /* /root/Event/referrer */ ,
  val pageName: String /* /root/Event/pageName */ , val requestUri: String /* /root/Event/requestUri */ ,
  /* ******************************************** */
  /* User attributes and fields */
  val user_ip: String /* /root/Event/user/ip */ , val user_userAgent: String /* /root/Event/user/userAgent */ ,
  val user_robot: String /* /root/Event/user/robot */ , val user_location: String /* /root/Event/user/location */ ,
  val user_browser: String /* /root/Event/user/browser */ ,
  /* ******************************************** */
  /* Search attributes and fields */
  val search_searchId: String /* /root/Event/search/searchId */ ,
  val search_what: String /* /root/Event/search/what */ , val search_where: String /* /root/Event/search/where */ ,
  val search_resultCount: String /* /root/Event/search/resultCount */ ,
  val search_resolvedWhat: String /* /root/Event/search/resolvedWhat */ ,
  val search_disambiguationPopup: String /* /root/Event/search/disambiguationPopup */ ,
  val search_dYMSuggestions: String /* /root/Event/search/dYMSuggestions */ ,
  val search_failedOrSuccess: String /* /root/Event/search/failedOrSuccess */ ,
  val search_hasRHSListings: String /* /root/Event/search/hasRHSListings */ ,
  val search_hasNonAdRollupListings: String /* /root/Event/search/hasNonAdRollupListings */ ,
  val search_calledBing: String /* /root/Event/search/calledBing */ , val search_geoORdir: String /* /root/Event/search/geoORdir */ ,
  val search_listingsCategoriesTiersMainListsAuxLists_category_id: String /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id */ ,
  val search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_id: String /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id */ ,
  val search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_count: String /* /root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count */ ,
  val search_matchedGeo_geo: String /* /root/Event/search/matchedGeo/geo */ ,
  val search_matchedGeo_type: String /* /root/Event/search/matchedGeo/type */ ,
  val search_matchedGeo_polygonIds: String /* /root/Event/search/matchedGeo/polygonIds */ ,
  val search_allListingsTypesMainLists: String /* /root/Event/search/allListingsTypesMainLists */ ,
  val search_directoriesReturned: String /* /root/Event/search/directoriesReturned */ ,
  val search_allHeadings_heading_name: String /* /root/Event/search/allHeadings/heading/name */ ,
  val search_allHeadings_heading_category: String /* /root/Event/search/allHeadings/heading/category */ ,
  val search_type: String /* /root/Event/search/type */ , val search_resultPage: String /* /root/Event/search/resultPage */ ,
  val search_resultPerPage: String /* /root/Event/search/resultPerPage */ , val search_latitude: String /* /root/Event/search/latitude */ ,
  val search_longitude: String /* /root/Event/search/longitude */ ,
  val search_merchants_attr_id: String /* /root/Event/search/merchants/@id */ ,
  val search_merchants_attr_zone: String /* /root/Event/search/merchants/@zone */ ,
  val search_merchants_attr_latitude: String /* /root/Event/search/merchants/@latitude */ ,
  val search_merchants_attr_longitude: String, /* /root/Event/search/merchants/@longitude */
  val search_merchants_attr_distance: String, /* /root/Event/search/merchants/@distance */
  val search_merchants_RHSorLHS: String /* /root/Event/search/merchants/RHSorLHS */ ,
  val search_merchants_isNonAdRollup: String /* /root/Event/search/merchants/isNonAdRollup */ ,
  val search_merchants_ranking: String /* /root/Event/search/merchants/ranking */ ,
  val search_merchants_isListingRelevant: String /* /root/Event/search/merchants/isListingRelevant */ ,
  val search_merchants_entry_heading_attr_isRelevant: String /* /root/Event/search/merchants/entry/heading/@isRelevant */ ,
  val search_merchants_entry_heading_categories: String /* /root/Event/search/merchants/entry/heading/categories */ ,
  val search_merchants_entry_directories_channel1: String /* /root/Event/search/merchants/entry/directories/channel1 */ ,
  val search_merchants_entry_directories_channel2: String /* /root/Event/search/merchants/entry/directories/channel2 */ ,
  val search_merchants_entry_product_productType: String /* /root/Event/search/merchants/entry/product/productType */ ,
  val search_merchants_entry_product_language: String /* /root/Event/search/merchants/entry/product/language */ ,
  val search_merchants_entry_product_udac: String /* /root/Event/search/merchants/entry/product/udac */ ,
  val search_merchants_entry_listingType: String /* /root/Event/search/merchants/entry/listingType */ ,
  val search_searchAnalysis_fuzzy: String /* /root/Event/search/searchAnalysis/fuzzy */ ,
  val search_searchAnalysis_geoExpanded: String /* /root/Event/search/searchAnalysis/geoExpanded */ ,
  val search_searchAnalysis_businessName: String /* /root/Event/search/searchAnalysis/businessName*/ ,
  /* ******************************************** */
  /* Search Analytics attributes and fields */
  val searchAnalytics_entry_attr_key: String /* /root/Event/searchAnalytics/entry/@key */ ,
  val searchAnalytics_entry_attr_value: String /* /root/Event/searchAnalytics/entry/@value */ ) extends Product with Serializable {
  override def toString = {
    s"""
         | attr_id (/root/Event/@id) = ${attr_id},
         | attr_timestamp (/root/Event/@timestamp) = ${attr_timestamp},
         | attr_site (/root/Event/@site) = ${attr_site},
         | attr_siteLanguage (/root/Event/@siteLanguage) = ${attr_siteLanguage},
         | attr_userId (/root/Event/@userId) = ${attr_userId},
         | attr_apiKey (/root/Event/apiKey) = ${attr_apiKey},
         | sessionId (/root/Event/sessionId) = ${sessionId},
         | transactionDuration (/root/Event/transactionDuration) = ${transactionDuration},
         | cachingUsed (/root/Event/cachingUsed) = ${cachingUsed},
         | referrer (/root/Event/referrer) = ${referrer},
         | pageName (/root/Event/pageName) = ${pageName},
         |  requestUri (/root/Event/requestUri) = ${requestUri},
         |  user_ip (/root/Event/user/ip) = ${user_ip},
         |  user_userAgent (/root/Event/user/userAgent) = ${user_userAgent},
         |  user_robot (/root/Event/user/robot) = ${user_robot},
         |  user_location (/root/Event/user/location) = ${user_location},
         |  search_searchId (/root/Event/search/searchId) = ${search_searchId},
         |  search_what (/root/Event/search/what) = ${search_what},
         |  search_where (/root/Event/search/where) = ${search_where},
         |  search_resultCount (/root/Event/search/resultCount) = ${search_resultCount},
         |  search_resolvedWhat (/root/Event/search/resolvedWhat) = ${search_resolvedWhat},
         |  search_disambiguationPopup (/root/Event/search/disambiguationPopup) = ${search_disambiguationPopup},
         |  search_dYMSuggestions (/root/Event/search/dYMSuggestions) = ${search_dYMSuggestions},
         |  search_failedOrSuccess (/root/Event/search/failedOrSuccess) = ${search_failedOrSuccess},
         |  search_hasRHSListings (/root/Event/search/hasRHSListings) = ${search_hasRHSListings},
         | search_hasNonAdRollupListings (/root/Event/search/hasNonAdRollupListings) = ${search_hasNonAdRollupListings},
         | search_calledBing (/root/Event/search/calledBing) = ${search_calledBing},
         | search_geoORdir (/root/Event/search/geoORdir) = ${search_geoORdir},
         | search_listingsCategoriesTiersMainListsAuxLists_category_id (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id) = ${search_listingsCategoriesTiersMainListsAuxLists_category_id},
         | search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_id (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id) = ${search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_id},
         | search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_count (/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count) = ${search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_count},
         | search_matchedGeo_geo (/root/Event/search/matchedGeo/geo) = ${search_matchedGeo_geo},
         | search_matchedGeo_type (/root/Event/search/matchedGeo/type) = ${search_matchedGeo_type},
         | search_matchedGeo_polygonIds (/root/Event/search/matchedGeo/polygonIds) = ${search_matchedGeo_polygonIds},
         | search_allListingsTypesMainLists (/root/Event/search/allListingsTypesMainLists) = ${search_allListingsTypesMainLists},
         | search_directoriesReturned (/root/Event/search/directoriesReturned) = ${search_directoriesReturned},
         | search_allHeadings_heading_name (/root/Event/search/allHeadings/heading/name) = ${search_allHeadings_heading_name},
         | search_allHeadings_heading_category (/root/Event/search/allHeadings/heading/category) = ${search_allHeadings_heading_category},
         | search_type (/root/Event/search/type) = ${search_type},
         | search_resultPage (/root/Event/search/resultPage) = ${search_resultPage},
         | search_resultPerPage (/root/Event/search/resultPerPage) = ${search_resultPerPage},
         | search_latitude (/root/Event/search/latitude) = ${search_latitude},
         | search_longitude (/root/Event/search/longitude) = ${search_longitude},
         | search_merchants_attr_id (/root/Event/search/merchants/@id) = ${search_merchants_attr_id},
         | search_merchants_attr_zone (/root/Event/search/merchants/@zone) = ${search_merchants_attr_zone},
         | search_merchants_attr_latitude (/root/Event/search/merchants/@latitude) = ${search_merchants_attr_latitude},
         | search_merchants_attr_longitude (/root/Event/search/merchants/@longitude) = ${search_merchants_attr_longitude},
         | search_merchants_attr_distance (/root/Event/search/merchants/@distance) = ${search_merchants_attr_distance},
         | search_merchants_RHSorLHS (/root/Event/search/merchants/RHSorLHS) = ${search_merchants_RHSorLHS},
         | search_merchants_isNonAdRollup (/root/Event/search/merchants/isNonAdRollup) = ${search_merchants_isNonAdRollup},
         | search_merchants_ranking (/root/Event/search/merchants/ranking) = ${search_merchants_ranking},
         | search_merchants_isListingRelevant (/root/Event/search/merchants/isListingRelevant) = ${search_merchants_isListingRelevant},
         | search_merchants_entry_heading_attr_isRelevant (/root/Event/search/merchants/entry/heading/@isRelevant) = ${search_merchants_entry_heading_attr_isRelevant},
         | search_merchants_entry_heading_categories (/root/Event/search/merchants/entry/heading/categories) = ${search_merchants_entry_heading_categories},
         | search_merchants_entry_directories_channel1 (/root/Event/search/merchants/entry/directories/channel1) = ${search_merchants_entry_directories_channel1},
         | search_merchants_entry_directories_channel2 (/root/Event/search/merchants/entry/directories/channel2) = ${search_merchants_entry_directories_channel2},
         | search_merchants_entry_product_productType (/root/Event/search/merchants/entry/product/productType) = ${search_merchants_entry_product_productType},
         | search_merchants_entry_product_language (/root/Event/search/merchants/entry/product/language) = ${search_merchants_entry_product_language},
         | search_merchants_entry_product_udac (/root/Event/search/merchants/entry/product/udac) = ${search_merchants_entry_product_udac},
         | search_merchants_entry_listingType (/root/Event/search/merchants/entry/listingType) = ${search_merchants_entry_listingType},
         | search_searchAnalysis_fuzzy (/root/Event/search/searchAnalysis/fuzzy) = ${search_searchAnalysis_fuzzy},
         | search_searchAnalysis_geoExpanded (/root/Event/search/searchAnalysis/geoExpanded) = ${search_searchAnalysis_geoExpanded},
         | search_searchAnalysis_businessName (/root/Event/search/searchAnalysis/businessName") = ${search_searchAnalysis_businessName},
         | searchAnalytics_entry_attr_key (/root/Event/searchAnalytics/entry/@key) = ${searchAnalytics_entry_attr_key},
         | searchAnalytics_entry_attr_value (/root/Event/searchAnalytics/entry/@value) = ${searchAnalytics_entry_attr_value}
         """.stripMargin
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[eventDetail]

  override def productArity: Int = 66

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int) = n match {
    case 0 => attr_id
    case 1 => attr_timestamp
    case 2 => attr_site
    case 3 => attr_siteLanguage
    case 4 => attr_userId
    case 5 => attr_apiKey
    case 6 => sessionId
    case 7 => transactionDuration
    case 8 => cachingUsed
    case 9 => referrer
    case 10 => pageName
    case 11 => requestUri
    case 12 => user_ip
    case 13 => user_userAgent
    case 14 => user_robot
    case 15 => user_location
    case 16 => user_browser
    case 17 => search_searchId
    case 18 => search_what
    case 19 => search_where
    case 20 => search_resultCount
    case 21 => search_resolvedWhat
    case 22 => search_disambiguationPopup
    case 23 => search_dYMSuggestions
    case 24 => search_failedOrSuccess
    case 25 => search_hasRHSListings
    case 26 => search_hasNonAdRollupListings
    case 27 => search_calledBing
    case 28 => search_geoORdir
    case 29 => search_listingsCategoriesTiersMainListsAuxLists_category_id
    case 30 => search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_id
    case 31 => search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_count
    case 32 => search_matchedGeo_geo
    case 33 => search_matchedGeo_type
    case 34 => search_matchedGeo_polygonIds
    case 35 => search_allListingsTypesMainLists
    case 36 => search_directoriesReturned
    case 37 => search_allHeadings_heading_name
    case 38 => search_allHeadings_heading_category
    case 39 => search_type
    case 40 => search_resultPage
    case 41 => search_resultPerPage
    case 42 => search_latitude
    case 43 => search_longitude
    case 44 => search_merchants_attr_id
    case 45 => search_merchants_attr_zone
    case 46 => search_merchants_attr_latitude
    case 47 => search_merchants_attr_longitude
    case 48 => search_merchants_attr_distance
    case 49 => search_merchants_RHSorLHS
    case 50 => search_merchants_isNonAdRollup
    case 51 => search_merchants_ranking
    case 52 => search_merchants_isListingRelevant
    case 53 => search_merchants_entry_heading_attr_isRelevant
    case 54 => search_merchants_entry_heading_categories
    case 55 => search_merchants_entry_directories_channel1
    case 56 => search_merchants_entry_directories_channel2
    case 57 => search_merchants_entry_product_productType
    case 58 => search_merchants_entry_product_language
    case 59 => search_merchants_entry_product_udac
    case 60 => search_merchants_entry_listingType
    case 61 => search_searchAnalysis_fuzzy
    case 62 => search_searchAnalysis_geoExpanded
    case 63 => search_searchAnalysis_businessName
    case 64 => searchAnalytics_entry_attr_key
    case 65 => searchAnalytics_entry_attr_value
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

}

object eventDetail {
  /**
   * TODO
   * @param aMap
   * @return
   */
  def apply(aMap: Map[String, List[String]]): List[eventDetail] = {
    def list2String(l: List[String]): String = {
      l.length match {
        case 0 => ""
        case 1 => l.head
        case _ => l.mkString(start = "{", sep = ",", end = "}")
      }
    }
    val headingsWithCats = aMap.getOrElse("/root/Event/search/allHeadings/heading/name", List("")) zip aMap.getOrElse("/root/Event/search/allHeadings/heading/category", List(""))
    headingsWithCats.foldLeft(List.empty[eventDetail]) {
      case (listOfEvents, (aHeading, itsCategory)) =>
        new eventDetail(
          search_allHeadings_heading_name = aHeading,
          search_allHeadings_heading_category = itsCategory,
          attr_id = list2String(aMap.getOrElse("/root/Event/@id", List.empty)),
          attr_timestamp = list2String(aMap.getOrElse("/root/Event/@timestamp", List.empty)),
          attr_site = list2String(aMap.getOrElse("/root/Event/@site", List.empty)),
          attr_siteLanguage = list2String(aMap.getOrElse("/root/Event/@siteLanguage", List.empty)),
          attr_userId = list2String(aMap.getOrElse("/root/Event/@userId", List.empty)),
          attr_apiKey = list2String(aMap.getOrElse("/root/Event/apiKey", List.empty)), sessionId = list2String(aMap.getOrElse("/root/Event/sessionId", List.empty)),
          transactionDuration = list2String(aMap.getOrElse("/root/Event/transactionDuration", List.empty)),
          cachingUsed = list2String(aMap.getOrElse("/root/Event/cachingUsed", List.empty)), referrer = list2String(aMap.getOrElse("/root/Event/referrer", List.empty)),
          pageName = list2String(aMap.getOrElse("/root/Event/pageName", List.empty)), requestUri = list2String(aMap.getOrElse("/root/Event/requestUri", List.empty)),
          /* ******************************************** */
          /* User attributes and fields */
          user_ip = list2String(aMap.getOrElse("/root/Event/user/ip", List.empty)), user_userAgent = list2String(aMap.getOrElse("/root/Event/user/userAgent", List.empty)),
          user_robot = list2String(aMap.getOrElse("/root/Event/user/robot", List.empty)), user_location = list2String(aMap.getOrElse("/root/Event/user/location", List.empty)),
          user_browser = list2String(aMap.getOrElse("/root/Event/user/browser", List.empty)),
          /* ******************************************** */
          /* Search attributes and fields */
          search_searchId = list2String(aMap.getOrElse("/root/Event/search/searchId", List.empty)),
          search_what = list2String(aMap.getOrElse("/root/Event/search/what", List.empty)), search_where = list2String(aMap.getOrElse("/root/Event/search/where", List.empty)),
          search_resultCount = list2String(aMap.getOrElse("/root/Event/search/resultCount", List.empty)),
          search_resolvedWhat = list2String(aMap.getOrElse("/root/Event/search/resolvedWhat", List.empty)),
          search_disambiguationPopup = list2String(aMap.getOrElse("/root/Event/search/disambiguationPopup", List.empty)),
          search_dYMSuggestions = list2String(aMap.getOrElse("/root/Event/search/dYMSuggestions", List.empty)),
          search_failedOrSuccess = list2String(aMap.getOrElse("/root/Event/search/failedOrSuccess", List.empty)),
          search_hasRHSListings = list2String(aMap.getOrElse("/root/Event/search/hasRHSListings", List.empty)),
          search_hasNonAdRollupListings = list2String(aMap.getOrElse("/root/Event/search/hasNonAdRollupListings", List.empty)),
          search_calledBing = list2String(aMap.getOrElse("/root/Event/search/calledBing", List.empty)), search_geoORdir = list2String(aMap.getOrElse("/root/Event/search/geoORdir", List.empty)),
          search_listingsCategoriesTiersMainListsAuxLists_category_id = list2String(aMap.getOrElse("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id", List.empty)),
          search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_id = list2String(aMap.getOrElse("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id", List.empty)),
          search_listingsCategoriesTiersMainListsAuxLists_category_id_tier_count = list2String(aMap.getOrElse("/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count", List.empty)),
          search_matchedGeo_geo = list2String(aMap.getOrElse("/root/Event/search/matchedGeo/geo", List.empty)),
          search_matchedGeo_type = list2String(aMap.getOrElse("/root/Event/search/matchedGeo/type", List.empty)),
          search_matchedGeo_polygonIds = list2String(aMap.getOrElse("/root/Event/search/matchedGeo/polygonIds", List.empty)),
          search_allListingsTypesMainLists = list2String(aMap.getOrElse("/root/Event/search/allListingsTypesMainLists", List.empty)),
          search_directoriesReturned = list2String(aMap.getOrElse("/root/Event/search/directoriesReturned", List.empty)),
          search_type = list2String(aMap.getOrElse("/root/Event/search/type", List.empty)), search_resultPage = list2String(aMap.getOrElse("/root/Event/search/resultPage", List.empty)),
          search_resultPerPage = list2String(aMap.getOrElse("/root/Event/search/resultPerPage", List.empty)), search_latitude = list2String(aMap.getOrElse("/root/Event/search/latitude", List.empty)),
          search_longitude = list2String(aMap.getOrElse("/root/Event/search/longitude", List.empty)),
          search_merchants_attr_id = list2String(aMap.getOrElse("/root/Event/search/merchants/@id", List.empty)),
          search_merchants_attr_zone = list2String(aMap.getOrElse("/root/Event/search/merchants/@zone", List.empty)),
          search_merchants_attr_latitude = list2String(aMap.getOrElse("/root/Event/search/merchants/@latitude", List.empty)),
          search_merchants_attr_longitude = list2String(aMap.getOrElse("/root/Event/search/merchants/@longitude", List.empty)),
          search_merchants_attr_distance = list2String(aMap.getOrElse("/root/Event/search/merchants/@distance", List.empty)),
          search_merchants_RHSorLHS = list2String(aMap.getOrElse("/root/Event/search/merchants/RHSorLHS", List.empty)),
          search_merchants_isNonAdRollup = list2String(aMap.getOrElse("/root/Event/search/merchants/isNonAdRollup", List.empty)),
          search_merchants_ranking = list2String(aMap.getOrElse("/root/Event/search/merchants/ranking", List.empty)),
          search_merchants_isListingRelevant = list2String(aMap.getOrElse("/root/Event/search/merchants/isListingRelevant", List.empty)),
          search_merchants_entry_heading_attr_isRelevant = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/heading/@isRelevant", List.empty)),
          search_merchants_entry_heading_categories = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/heading/categories", List.empty)),
          search_merchants_entry_directories_channel1 = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/directories/channel1", List.empty)),
          search_merchants_entry_directories_channel2 = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/directories/channel2", List.empty)),
          search_merchants_entry_product_productType = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/product/productType", List.empty)),
          search_merchants_entry_product_language = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/product/language", List.empty)),
          search_merchants_entry_product_udac = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/product/udac", List.empty)),
          search_merchants_entry_listingType = list2String(aMap.getOrElse("/root/Event/search/merchants/entry/listingType", List.empty)),
          search_searchAnalysis_fuzzy = list2String(aMap.getOrElse("/root/Event/search/searchAnalysis/fuzzy", List.empty)),
          search_searchAnalysis_geoExpanded = list2String(aMap.getOrElse("/root/Event/search/searchAnalysis/geoExpanded", List.empty)),
          search_searchAnalysis_businessName = list2String(aMap.getOrElse("/root/Event/search/searchAnalysis/businessName", List.empty)),
          /* ******************************************** */
          /* Search Analytics attributes and fields */
          searchAnalytics_entry_attr_key = list2String(aMap.getOrElse("/root/Event/searchAnalytics/entry/@key", List.empty)),
          searchAnalytics_entry_attr_value = list2String(aMap.getOrElse("/root/Event/searchAnalytics/entry/@value", List.empty))) :: listOfEvents
    }

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
            val rddMap = BasicParser.parseEvent(xml = new XMLEventReader(theSource), startXPath = XPath("/root/Event"), eventIdOpt = None)
            rddMap
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

