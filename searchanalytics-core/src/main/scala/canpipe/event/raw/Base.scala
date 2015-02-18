package canpipe.event.raw

/**
 * Represents an event in Canpipe. We call this 'raw' as this is *exactly* the structure we observe
 * in the XML file.
 */
case class Event(
  id: String,
  timestamp: String,
  site: String,
  siteLanguage: String,
  eventType: String,
  isMap: String,
  userId: String,
  typeOfLog: String,
  apiKey: String,
  sessionId: String,
  transactionDuration: String,
  cachingUsed: String,
  applicationId: String,
  referrer: String,
  pageName: String,
  requestUri: String,
  user: User,
  search: Option[SearchInfo],
  searchAnalytics: SearchAnalytics) extends Serializable {

  // TODO: complete this definition
  override def toString = {
    s"""
         | id = '${id}', timestamp = '${timestamp}', site = '${site}',
         |  user info = ${user.toString},
         |  search info = ${if (search.isDefined) search.get.toString else "*** Not Defined ***"},
         | search Analytics Info = ${searchAnalytics.toString}
         """.stripMargin
  }

}

/* User attributes and fields */
case class User(id: String, ip: String, deviceId: String, userAgent: String, browser: String, browserAgent: String, os: String) extends Serializable

case class ListingCategory(id: String)
case class ListingsCategoriesTiersMainListsAuxLists(category: Set[ListingCategory])

case class SearchHeading(name: String, category: String)
case class SearchHeadings(headings: Set[SearchHeading])

case class EntryHeading(isRelevant: String, categories: String)
case class EntryDirectories(channel1: String)

case class MerchantEntry(headings: Set[EntryHeading], directories: Set[EntryDirectories], listingType: String)

case class Merchant(id: String, zone: String, latitude: String, longitude: String, distance: String, ranking: String, isListingRelevant: String, entry: MerchantEntry)

case class SearchAnalysis(fuzzy: String, geoExpanded: String, businessName: String)

/* Search attributes and fields */
class SearchInfo(
  val searchId: String, val what: String, val where: String, val resultCount: String, val resolvedWhat: String,
  val whereFinal: String, val searchBoxUsed: String,
  val disambiguationPopup: String, val failedOrSuccess: String, val hasRHSListings: String,
  val hasNonAdRollupListings: String, val calledBing: String,
  val geoORDir: String,
  val listingsCategoriesTiersMainListsAuxLists: ListingsCategoriesTiersMainListsAuxLists,
  val matchedGeo: String,
  val allListingsTypesMainLists: String,
  val relatedListingsReturned: String,
  val directoriesReturned: String,
  val allHeadings: SearchHeadings,
  val searchtype: String,
  val resultPage: String,
  val resultPerPage: String,
  val latitude: String,
  val longitude: String,
  val radius_1: String,
  val radius_2: String,
  val merchants: Set[Merchant],
  val searchAnalysis: SearchAnalysis) extends Serializable {

  // TODO: complete
  override def toString = {
    s"""
         | searchId = '${searchId}', what = '${what}', what = '${what}',
         |  where = '${where}',
         |  allHeadings has ${allHeadings.headings.size} elements
         |  merchants has ${merchants.size} elements
         """.stripMargin
  }

}

/* Search Analytics/Analysis attributes and fields */
case class SearchAnalyticsEntry(key: String, value: String) extends Serializable
case class SearchAnalytics(entries: Set[SearchAnalyticsEntry]) extends Serializable

