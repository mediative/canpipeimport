package util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.xml.pull._

// import com.typesafe.scalalogging.Logging
// import com.typesafe.scalalogging.slf4j.StrictLogging

object Base { // extends Logging {

  object CanPipe {
    object FieldImportance extends Enumeration {
      type FieldImportance = Value
      val Must, Should, Could = Value
    }
    import FieldImportance._

    val fieldsDef: Map[String, FieldImportance] = Map(
      "/root/Event/@id" -> Must,
      "/root/Event/@timestamp" -> Must,
      "/root/Event/@site" -> Must,
      "/root/Event/@siteLanguage" -> Must,
      "/root/Event/@eventType" -> Must, // Two values are possible "impression" which is a SERP event, or "click" which is an MP event
      "/root/Event/@userId" -> Should,
      "/root/Event/apiKey" -> Should, "/root/Event/sessionId" -> Must, "/root/Event/transactionDuration" -> Could,
      "/root/Event/cachingUsed" -> Could, "/root/Event/referrer" -> Must, "/root/Event/user/ip" -> Could, "/root/Event/user/userAgent" -> Must,
      "/root/Event/user/robot" -> Must, "/root/Event/user/location" -> Must, "/root/Event/user/browser" -> Should,
      "/root/Event/search/searchId" -> Must,
      "/root/Event/search/what" -> Must, "/root/Event/search/where" -> Must, "/root/Event/search/resultCount" -> Must,
      "/root/Event/search/resolvedWhat" -> Must, "/root/Event/search/disambiguationPopup" -> Should,
      "/root/Event/search/dYMSuggestions" -> Should,
      "/root/Event/search/failedOrSuccess" -> Must, "/root/Event/search/hasRHSListings" -> Must,
      "/root/Event/search/hasNonAdRollupListings" -> Must, "/root/Event/search/calledBing" -> Should, "/root/Event/search/geoORdir" -> Must,
      "/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id" -> Should,
      "/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/id" -> Should,
      "/root/Event/search/listingsCategoriesTiersMainListsAuxLists/category/id/tier/count" -> Should,
      "/root/Event/search/matchedGeo/geo" -> Must, "/root/Event/search/matchedGeo/type" -> Must, "/root/Event/search/matchedGeo/polygonIds" -> Must,
      "/root/Event/search/allListingsTypesMainLists" -> Must, "/root/Event/search/directoriesReturned" -> Must, "/root/Event/search/allHeadings/heading/name" -> Should,
      "/root/Event/search/allHeadings/heading/category" -> Should, "/root/Event/search/type" -> Must, "/root/Event/search/resultPage" -> Must,
      "/root/Event/search/resultPerPage" -> Must, "/root/Event/search/latitude" -> Must, "/root/Event/search/longitude" -> Must,
      "/root/Event/search/merchants/@id" -> Must, // Merchant ID (MID)
      "/root/Event/search/merchants/@zone" -> Must,
      "/root/Event/search/merchants/@latitude" -> Must,
      "/root/Event/search/merchants/@longitude" -> Must,
      "/root/Event/search/merchants/@distance" -> Must,
      "/root/Event/search/merchants/RHSorLHS" -> Must, "/root/Event/search/merchants/isNonAdRollup" -> Must, "/root/Event/search/merchants/ranking" -> Must,
      "/root/Event/search/merchants/isListingRelevant" -> Must,
      "/root/Event/search/merchants/entry/heading/@isRelevant" -> Should,
      "/root/Event/search/merchants/entry/heading/categories" -> Should, "/root/Event/search/merchants/entry/directories/channel1" -> Should,
      "/root/Event/search/merchants/entry/directories/channel2" -> Should,
      "/root/Event/search/merchants/entry/product/productType" -> Should, "/root/Event/search/merchants/entry/product/language" -> Should,
      "/root/Event/search/merchants/entry/product/udac" -> Should, "/root/Event/search/merchants/entry/listingType" -> Must,
      "/root/Event/search/searchAnalysis/fuzzy" -> Must, "/root/Event/search/searchAnalysis/geoExpanded" -> Must, "/root/Event/search/searchAnalysis/businessName" -> Must,
      "/root/Event/pageName" -> Must, "/root/Event/requestUri" -> Must,
      "/root/Event/searchAnalytics/entry/@key" -> Must,
      "/root/Event/searchAnalytics/entry/@value" -> Must)

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
      val searchAnalytics_entry_attr_value: String /* /root/Event/searchAnalytics/entry/@value */ ) {
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
    }

    case class headingEntry(event_id: String, heading: String, category: String)
    object HeadingReadingStatus extends Enumeration {
      type HeadingReadingStatus = Value
      val NONE, READING_HEADING, READING_NAME, READING_CATEGORY = Value
    }
    import HeadingReadingStatus._

    object eventDetail {
      def apply(aMap: Map[String, List[String]]): eventDetail = {
        def list2String(l: List[String]): String = {
          l.length match {
            case 0 => ""
            case 1 => l.head
            case _ => l.mkString(start = "{", sep = ",", end = "}")
          }
        }
        new eventDetail(
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
          search_allHeadings_heading_name = list2String(aMap.getOrElse("/root/Event/search/allHeadings/heading/name", List.empty)),
          search_allHeadings_heading_category = list2String(aMap.getOrElse("/root/Event/search/allHeadings/heading/category", List.empty)),
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
          searchAnalytics_entry_attr_value = list2String(aMap.getOrElse("/root/Event/searchAnalytics/entry/@value", List.empty)))

      }
    }

    case class XPath(s: String) {
      def asString = s
    }
    object XPath {
      private[util] val SEP = "/"
      private[util] def fromReverseNameList(reverseNameList: List[String]): String =
        reverseNameList.reverse.mkString(start = SEP, sep = SEP, end = "")
      private[util] def toReverseNameList(s: String): List[String] = {
        { if (s.startsWith(SEP)) s.substring(1) else s }.split(SEP).reverse.toList
      }
      // def apply(s: String): XPath = { if (s.startsWith(SEP)) (new XPath(s)) else (new XPath(SEP + s)) }
      def add(aPath: XPath, aPart: String) = XPath(s"${aPath.s}${SEP}${aPart}")
      def removeLast(aPath: XPath) = {
        XPath(fromReverseNameList(toReverseNameList(aPath.s).tail))
      }
    }

    // TODO: put this in proper location
    trait EventGroup {
      def eventsAsString: RDD[String]
    }

    case class EventGroupFromHDFSFile(sc: SparkContext, hdfsFileName: String) extends EventGroup {
      val eventsAsString: RDD[String] = sc.textFile(hdfsFileName).filter(!_.endsWith("root>"))
    }

    // TODO: put this in proper location
    trait FilterRule {
      def name: String
      def values: Set[String]
    }

    case class AcceptOnlyRule(name: String, values: Set[String]) extends FilterRule
    case class RejectRule(name: String, values: Set[String]) extends FilterRule

    class CanPipeParser(val filterRules: Set[FilterRule]) {
      def this() = this(Set.empty)
      def parseFromResources(fileName: String) = CanPipeParser.parseFromResources(fileName, filterRules)
    }

    object CanPipeParser {

      /**
       * Cycles through the text until there is no more to consume.
       * @param partsOfTextInt
       * @return
       */
      private[util] def processText(xml: XMLEventReader, partsOfTextInt: List[String]): (Boolean, String) = {
        if (xml.hasNext) {
          xml.next match {
            case EvElemStart(pre, label, attrs, scope) =>
              println("HOW DID THIS HAPPENED???????????????????")
              // logger.error("HOW DID THIS HAPPENED???????????????????")
              // TODO: obviously throwing this Exception is only acceptable because this is a throw-away solution
              throw new RuntimeException("HOW ON EARTH DID THIS HAPPEN????????? ==> we are parsing the TEXT and a new EVENT is being opened!!")
            case EvElemEnd(_, label) =>
              (label == "Event", partsOfTextInt.mkString("&"))
            case EvText(text) =>
              // logger.info(s"TEXT ===> ${text}")
              processText(xml, partsOfTextInt ++ List(text))
            case _ => {
              processText(xml, partsOfTextInt)
            }
          }
        } else
          (true, partsOfTextInt.mkString("&"))
      }

      /**
       *
       * @note We assume we just entered the label 'allHeadings'
       * @param headings
       * @return
       */
      private[util] def processAllHeadingsForEvent(xml: XMLEventReader, eventId: String, headings: List[headingEntry]): List[headingEntry] = {
        def loop(headings: List[headingEntry], readingStatus: HeadingReadingStatus): List[headingEntry] = {
          if (xml.hasNext) {
            xml.next match {
              case EvElemStart(pre, label, attrs, scope) =>
                // logger.info(s"start -> '${label}'")
                label match {
                  case "heading" => loop(headings, READING_HEADING)
                  case "name" if (readingStatus == READING_HEADING) =>
                    loop(headings, READING_NAME)
                  case "category" if (readingStatus == READING_HEADING) =>
                    loop(headings, READING_CATEGORY)
                  case _ => loop(headings, readingStatus)
                }
              case EvElemEnd(_, label) =>
                // logger.info(s"end -> '${label}'")
                label match {
                  case "allHeadings" => headings
                  case "name" | "category" => loop(headings, readingStatus = READING_HEADING)
                  case _ => loop(headings, readingStatus = NONE)
                }
              case EvText(text) =>
                readingStatus match {
                  case READING_NAME => {
                    // logger.info(s"READING_NAME -> '${text}'")
                    // add a new entry, with no category
                    loop(headingEntry(event_id = eventId, heading = text, category = "") :: headings, readingStatus)
                  }
                  case READING_CATEGORY => {
                    // logger.info(s"READING_CATEGORY -> '${text}'")
                    // adds the category to the latest entry:
                    loop(headings.head.copy(category = text) :: headings.tail, readingStatus)
                  }
                  case _ => {
                    println(s"NO STATUS.... -> '${text}'")
                    // TODO logger.info(s"NO STATUS.... -> '${text}'")
                    loop(headings, readingStatus)
                  }
                }
              case _ => loop(headings, readingStatus)
            }
          } else {
            println(s"FINISHED PROCESSIN HEADIGNS, FOUND ${headings.size} OF THEM")
            // TODO: logger.info(s"FINISHED PROCESSIN HEADIGNS, FOUND ${headings.size} OF THEM")
            headings
          }
        }
        loop(headings, HeadingReadingStatus.NONE)
      }

      /**
       * Parses an impression
       * @param comingFromXPathAsReverseList
       * @return fieldLabel -> {values this label takes}
       */
      def parseEvent(xml: XMLEventReader, eventId: String,
                     startXPath: XPath): (Map[String, List[String]], List[headingEntry]) = {

        // println(s"parseEvent(${eventId}}): startXPath: ${startXPath.asString}")
        def loop(sourceXPath: XPath, resultMap: Map[String, List[String]],
                 headings: List[headingEntry]): (Map[String, List[String]], List[headingEntry]) = {
          // println(s"\t parseEvent.loop: sourceXPath: ${sourceXPath.asString}")

          if (xml.hasNext) {
            xml.next match {
              case EvElemStart(pre, label, attrs, scope) => {
                label match {
                  case "Event" =>
                    // TODO: obviously throwing this Exception is only acceptable because this is a throw-away solution
                    throw new RuntimeException("HOW ON EARTH DID THIS HAPPEN????????? ==> we are parsing an EVENT and another EVENT is being opened!!")
                  case "allHeadings" =>
                    loop(sourceXPath, resultMap, processAllHeadingsForEvent(xml, eventId, headings))
                  case _ =>
                    val currentXPath = XPath.add(sourceXPath, label)
                    val attrsMap = attrs.asAttrMap
                    val newCurrentMap =
                      // logger.info(s"Start element: pre = ${pre}, label = ${label}")
                      attrsMap.foldLeft(resultMap) {
                        case (currentResultMap, (attrName, attrValue)) =>
                          val attrLabel = "@" + attrName
                          val fieldLabel = XPath.add(currentXPath, attrLabel).asString
                          if (fieldsDef.contains(fieldLabel)) {
                            // println(s"\t **** Found [${fieldLabel}] ==> '${label}' metadata field '${attrName}' = '${attrValue}'")
                            currentResultMap + (fieldLabel -> (currentResultMap.getOrElse(fieldLabel, List.empty) ++ List(attrValue)))
                          } else {
                            // logger.info(s"\t NOT INTERESTING [${fieldLabel}] ==> '${label}' metadata field '${attrName}' = '${attrValue}'")
                            currentResultMap
                          }
                      }
                    loop(currentXPath, newCurrentMap,
                      headings)
                }
              }
              case EvElemEnd(_, label) =>
                if (label == "Event") {
                  // end of the parsing of the event
                  // println(s"end of the parsing of the event. Result Map has ${resultMap.size} entries")
                  (resultMap, headings)
                } else {
                  loop(XPath.removeLast(sourceXPath), resultMap, headings)
                }
              case EvText(text) =>
                // logger.info(s"TEXT ===> ${text}")
                val (reachedEndOfEvent, fieldValue) = processText(xml, List(text))
                if (reachedEndOfEvent)
                  (resultMap, headings)
                else {
                  loop(XPath.removeLast(sourceXPath), // TODO: or sourceXPath?????
                    {
                      val fieldLabel = sourceXPath.asString
                      // println(s"\t **** Found [${fieldLabel}] = '${fieldValue}'")
                      if (fieldsDef.contains(fieldLabel)) {
                        resultMap + (fieldLabel -> (resultMap.getOrElse(fieldLabel, List.empty) ++ List(fieldValue)))
                      } else
                        resultMap
                    }, headings)
                }
              case _ => {
                loop(sourceXPath, resultMap, headings)
              }
            }
          } else
            (resultMap, headings)
        }
        val (fieldsInImpression, theHeadings) = loop(startXPath, resultMap = Map.empty, headings = List.empty)
        (fieldsInImpression, theHeadings)
      }

      /*
      def parseEventGroup(events: EventGroup, filterRules: Set[FilterRule]): RDD[(Map[String, scala.List[String]], List[CanPipe.headingEntry])] = {
        events.eventsAsString.map { anEventAsString =>

          val bla =
            Source.fromString(anEventAsString) match {
              case x if (x == null) => {
                println(s"'Error building a Source from a particular XML event")
                (Set.empty, List.empty)
              }
              case theSource => {
                val (rddMap, rddHeadings) = parseImpressions(new XMLEventReader(theSource))

              }
            }

        }
      }
      */

      // reference: http://stackoverflow.com/questions/13184212/parsing-very-large-xml-lazily
      def parseFromResources(fileName: String, filterRules: Set[FilterRule]): (Set[Map[String, scala.List[String]]], scala.List[CanPipe.headingEntry]) = {

        def parseAllEventsFrom(xml: XMLEventReader): (Set[Map[String, List[String]]], List[headingEntry]) = {

          /**
           *
           * @param comingFromXPathAsReverseList
           * @param resultMap
           * @return a Set, where each element is an Impression Event, coded like this:
           *         fieldLabel -> {values this label takes}
           */
          def loop(sourceXPath: XPath, resultMap: Set[Map[String, List[String]]], headings: List[headingEntry]): (Set[Map[String, List[String]]], List[headingEntry]) = {
            if (xml.hasNext) {
              xml.next match {
                case EvElemStart(pre, label, attrs, scope) =>
                  val currentXPath = XPath.add(sourceXPath, label)
                  if (label == "Event") {
                    // if ((label == "Event") && (attrs.asAttrMap.getOrElse("eventType", "noneEventType") == "impression")) {
                    val (eventFields, eventHeadings) = {
                      // parse attributes of the event:
                      val attrsMap = attrs.asAttrMap
                      attrsMap.find { case (attrName, _) => attrName == "id" } match {
                        case None =>
                          println("No [id] for event!!! Ignoring.")
                          // logger.info("No [id] for impression!!! Ignoring.")
                          (Map.empty[String, scala.List[String]], headings)
                        case Some(idNameAndValue) =>
                          val (eventMap, eventHeadings) = parseEvent(xml, eventId = idNameAndValue._2, currentXPath)
                          // println(s"parsed event ${idNameAndValue._2} ==> ${eventMap.size} fields")
                          // eventMap.foreach { case (k, v) => println(s"${k} = ${v}") }
                          val allHeadings = eventHeadings ++ headings
                          (attrsMap.foldLeft(eventMap) {
                            case (currentResultMap, (attrName, attrValue)) =>
                              val attrLabel = "@" + attrName
                              val fieldLabel = XPath.add(currentXPath, attrLabel).asString
                              if (fieldsDef.contains(fieldLabel)) {
                                // logger.info(s"\t **** Found [${fieldLabel}] ==> '${label}' metadata field '${attrName}' = '${attrValue}'")
                                currentResultMap + (fieldLabel -> (currentResultMap.getOrElse(fieldLabel, List.empty) ++ List(attrValue)))
                              } else {
                                // logger.info(s"\t NOT INTERESTING [${fieldLabel}] ==> '${label}' metadata field '${attrName}' = '${attrValue}'")
                                currentResultMap
                              }
                          }, allHeadings)
                      }
                    }
                    // println(s"resultMap has size ${resultMap.size}")
                    loop(sourceXPath, resultMap + eventFields, eventHeadings)
                  } else {
                    loop(sourceXPath, resultMap, headings)
                  }
                case _ => loop(sourceXPath, resultMap, headings)
              }
            } else {
              (resultMap, headings)
            }
          }

          loop(XPath("/root"), resultMap = Set.empty, headings = List.empty)
        }
        Source.fromURL(getClass.getResource(s"/${fileName}")) match {
          case x if (x == null) => {
            println(s"'${fileName}' does not exist in resources.")
            (Set.empty, List.empty)
          }
          case theSource => {
            parseAllEventsFrom(new XMLEventReader(theSource))
          }
        }

      }
    }

  }

  def main(args: Array[String]) {
    import CanPipe._
    import CanPipe.FieldImportance._

    // // ("/sample.50.xml"))) // ("/sample1Impression.xml"))) //  // ("/analytics.log.2014-06-01-15")))

    def timeInMs[R](block: => R): (Long, R) = {
      val t0 = System.currentTimeMillis()
      val result = block // call-by-name
      val t1 = System.currentTimeMillis()
      ((t1 - t0), result)
    }
    // define filtering rules for this XML:
    val filterRules: Set[FilterRule] = Set.empty // TODO 
    //
    val myParser = new CanPipeParser(filterRules)
    println("COMPUTATION STARTED")
    val (timeItTook, (setOfImpressions, headings)) = timeInMs(myParser.parseFromResources("sample.50.xml"))
    println(s"Computation finished in ${timeItTook} ms.")

    val listOfMissing: List[(FieldImportance.Value, Map[String, Long])] =
      List(Must, Should).map { importanceLevel =>
        (importanceLevel,
          setOfImpressions.foldLeft(Map.empty[String, Long]) {
            case (mapOfMissing, mapOfResult) =>
              (fieldsDef.filter { case (_, importance) => importance == importanceLevel }.keySet -- mapOfResult.keySet).foldLeft(mapOfMissing) { (theMap, aFieldName) =>
                theMap + (aFieldName -> (theMap.getOrElse(aFieldName, 0L) + 1))
              }
          })
      }
    // report of SHOULD/MUST fields that are missing:
    println(s"**************  Report of fields that are missing, on the sampling of ${setOfImpressions.size} impressions **************")
    listOfMissing.foreach {
      case (importanceLevel, report) =>
        println(s"**************  Report of ${importanceLevel} fields that are missing  **************")
        report.foreach {
          case (aFieldName, howManyTimesMissing) =>
            println(s"'${aFieldName}', missed ${howManyTimesMissing} times")
        }
    }

    println(s"Table [headings] has ${headings.size} entries")

    println(s"'${setOfImpressions.size} IMPRESSION events parsed  ==> ")
    setOfImpressions.foreach { mapOfResult =>
      // display all:
      println("**************  Report of ALL fields  **************")
      mapOfResult.foreach {
        case (fieldName, listOfValues) =>
          println(s"\t 'field = ${fieldName}', value = ${listOfValues.mkString(start = "{", sep = ",", end = "}")}")
      }
      // val eD = eventDetail(mapOfResult)
      // logger.info(s"${eD.toString}")
    }

  }
}
