package canpipe.parser

import canpipe.xml.{ Elem => CanpipeElem }
import scales.utils._
import ScalesUtils._ // import implicits
import scales.xml._
import ScalesXml._ // import implicits
import canpipe.event.{ raw => rawevent }
import canpipe.event.raw.{ Event => RawEvent }

object Base {

  trait Outputable[A, B] {
    def validateFromRaw: RawEvent => Option[A]
    def cumulate: Option[A] => B => B
    def zeroAcc: B
  }

  object Parser {

    /**
     * All tags appearing in Canpipe's XML
     */
    private object tags {
      val root = NoNamespaceQName("root")
      //
      object event {
        val base = NoNamespaceQName("Event")
        // event -> @'s
        val id = NoNamespaceQName("id")
        val timestamp = NoNamespaceQName("timestamp")
        val site = NoNamespaceQName("site")
        val siteLanguage = NoNamespaceQName("siteLanguage")
        val eventType = NoNamespaceQName("id")
        val isMap = NoNamespaceQName("isMap")
        val userId = NoNamespaceQName("userId")
        val typeOfLog = NoNamespaceQName("typeOfLog")
        //
        val apiKey = NoNamespaceQName("apiKey")
        val sessionId = NoNamespaceQName("sessionId")
        val transactionDuration = NoNamespaceQName("transactionDuration")
        val cachingUsed = NoNamespaceQName("cachingUsed")
        val applicationId = NoNamespaceQName("applicationId")
        val referrer = NoNamespaceQName("referrer")
        val requestURI = NoNamespaceQName("requestUri")
        val pageName = NoNamespaceQName("pageName")

        object user {
          val base = NoNamespaceQName("user")
          val id = NoNamespaceQName("id")
          val ip = NoNamespaceQName("ip")
          val deviceId = NoNamespaceQName("deviceId")
          val userAgent = NoNamespaceQName("userAgent")
          val browser = NoNamespaceQName("browser")
          val browserVersion = NoNamespaceQName("browserVersion")
          val os = NoNamespaceQName("os")
        }

        object search {
          val base = NoNamespaceQName("search")
          val searchId = NoNamespaceQName("searchId")
          val what = NoNamespaceQName("what")
          val where = NoNamespaceQName("where")
          val resultCount = NoNamespaceQName("resultCount")
          val resolvedWhat = NoNamespaceQName("resolvedWhat")
          val whereFinal = NoNamespaceQName("whereFinal")
          val searchBoxUsed = NoNamespaceQName("searchBoxUsed")
          val disambiguationPopup = NoNamespaceQName("disambiguationPopup")
          val failedOrSuccess = NoNamespaceQName("failedOrSuccess")
          val termsUsedInNonMatch = NoNamespaceQName("termsUsedInNonMatch")
          val hasRHSListings = NoNamespaceQName("hasRHSListings")
          val hasNonAdRollupListings = NoNamespaceQName("hasNonAdRollupListings")
          val calledBing = NoNamespaceQName("calledBing")
          val geoORdir = NoNamespaceQName("geoORdir")
          object listingsCategoriesTiersMainListsAuxLists {
            val base = NoNamespaceQName("listingsCategoriesTiersMainListsAuxLists")
            object category {
              val base = NoNamespaceQName("category")
              val id = NoNamespaceQName("id")
            }
          }
          val matchedGeo = NoNamespaceQName("matchedGeo")
          val allListingsTypesMainLists = NoNamespaceQName("allListingsTypesMainLists")
          val relatedListingsReturned = NoNamespaceQName("relatedListingsReturned")
          val directoriesReturned = NoNamespaceQName("directoriesReturned")
          object allHeadings {
            val base = NoNamespaceQName("allHeadings")
            object heading {
              val base = NoNamespaceQName("heading")
              val name = NoNamespaceQName("name")
              val category = NoNamespaceQName("category")
            }
          }
          val searchtype = NoNamespaceQName("type")
          val resultPage = NoNamespaceQName("resultPage")
          val resultPerPage = NoNamespaceQName("resultPerPage")
          val latitude = NoNamespaceQName("latitude")
          val longitude = NoNamespaceQName("longitude")
          val radius_1 = NoNamespaceQName("radius_1")
          val radius_2 = NoNamespaceQName("radius_2")
          object merchants {
            val base = NoNamespaceQName("merchants")
            // @'s
            val id = NoNamespaceQName("id")
            val zone = NoNamespaceQName("zone")
            val latitude = NoNamespaceQName("latitude")
            val longitude = NoNamespaceQName("longitude")
            val distance = NoNamespaceQName("distance")
            //
            val ranking = NoNamespaceQName("ranking")
            val isListingRelevant = NoNamespaceQName("isListingRelevant")
            object entry {
              val base = NoNamespaceQName("entry")
              object headings {
                val base = NoNamespaceQName("headings")
                val isRelevant = NoNamespaceQName("isRelevant")
                val categories = NoNamespaceQName("categories")
              }
              object directories {
                val base = NoNamespaceQName("directories")
                val channel1 = NoNamespaceQName("channel1")
              }
              val listingType = NoNamespaceQName("listing1")
            }
          }
          object searchAnalysis {
            val base = NoNamespaceQName("searchAnalysis")
            val fuzzy = NoNamespaceQName("fuzzy")
            val geoExpanded = NoNamespaceQName("geoExpanded")
            val businessName = NoNamespaceQName("businessName")
          }
        }

        object searchAnalytics {
          val base = NoNamespaceQName("searchAnalytics")
          object entry {
            val base = NoNamespaceQName("entry")
            val key = NoNamespaceQName("key")
            val value = NoNamespaceQName("value")
          }
        }
        object logApi {
          val base = NoNamespaceQName("logApi")
          val requestURL = NoNamespaceQName("requestUrl")
          val listId = NoNamespaceQName("listId")
          val clickType = NoNamespaceQName("clickType")
        }
      }
    }

    def parse[A, B](anXMLNode: CanpipeElem)(implicit ops: Outputable[A, B]): B = {
      import java.io.StringReader
      import scala.xml.InputSource
      val inputSource = new InputSource(new StringReader(anXMLNode.value.toString()))
      val xml = pullXml(inputSource)
      parse(xml)
    }

    def parseFromResource[A, B](resourcePath: String)(implicit ops: Outputable[A, B]): Option[B] = {
      try {
        Some(parse(pullXml(resource(this, resourcePath))))
      } catch {
        case e: Exception => None // thrown when, eg, 'resourcePath' does not exist
      }
    }

    private def parse[A, B](xml: XmlPull)(implicit ops: Outputable[A, B]): B = {

      import tags.event.search.{ base => searchSection }
      import tags.event.search
      import tags.event.search.searchAnalysis.{ base => searchAnalysisSection }
      import tags.event.search.allHeadings.{ base => allHeadingsSection }
      import tags.event.search.merchants.{ base => merchantsSection }
      import tags.event.search.merchants
      import tags.event.search.allHeadings.heading.{ base => headingSection }
      import tags.event.search.allHeadings.heading
      import tags.event.user.{ base => userSection }
      import tags.event.user
      import tags.event.searchAnalytics.{ base => searchAnalyticsSection }
      import tags.event.searchAnalytics.entry.{ base => searchAnalyticsEntry }
      import tags.event.searchAnalytics.entry

      val EventPath = List(tags.root, tags.event.base)
      // iterate over each Event (only an Event at a time is in memory)
      iterate(EventPath, xml).foldLeft(ops.zeroAcc) { (acc, event) =>
        val theRawEvent =
          rawevent.Event(
            id = text(event.\.*@(tags.event.id)),
            timestamp = text(event.\.*@(tags.event.timestamp)),
            site = text(event.\.*@(tags.event.site)),
            siteLanguage = text(event.\.*@(tags.event.siteLanguage)),
            eventType = text(event.\.*@(tags.event.eventType)),
            isMap = text(event.\.*@(tags.event.isMap)),
            userId = text(event.\.*@(tags.event.userId)),
            typeOfLog = text(event \* tags.event.typeOfLog),
            apiKey = text(event \* tags.event.apiKey),
            sessionId = text(event \* tags.event.sessionId),
            transactionDuration = text(event \* tags.event.transactionDuration),
            cachingUsed = text(event \* tags.event.cachingUsed),
            applicationId = text(event \* tags.event.applicationId),
            referrer = text(event \* tags.event.referrer),
            requestUri = text(event \* tags.event.requestURI),
            pageName = text(event \* tags.event.pageName),
            user = {
              val theUserSection = event \* userSection
              rawevent.User(
                id = text(theUserSection \* user.id),
                ip = text(theUserSection \* user.ip),
                deviceId = text(theUserSection \* user.deviceId),
                userAgent = text(theUserSection \* user.userAgent),
                browser = text(theUserSection \* user.browser),
                browserAgent = text(theUserSection \* user.browserVersion),
                os = text(theUserSection \* user.os))
            },
            search = {
              val searchS = event \* searchSection
              val searchInfo: Option[rawevent.SearchInfo] = {
                if (searchS.isEmpty) None
                else {
                  Some(new rawevent.SearchInfo(
                    searchId = text(searchS \* search.searchId),
                    what = text(searchS \* search.what),
                    where = text(searchS \* search.where),
                    resultCount = text(searchS \* search.resultCount),
                    resolvedWhat = text(searchS \* search.resolvedWhat),
                    whereFinal = text(searchS \* search.whereFinal),
                    searchBoxUsed = text(searchS \* search.searchBoxUsed),
                    disambiguationPopup = text(searchS \* search.disambiguationPopup),
                    failedOrSuccess = text(searchS \* search.failedOrSuccess),
                    hasRHSListings = text(searchS \* search.hasRHSListings),
                    hasNonAdRollupListings = text(searchS \* search.resolvedWhat),
                    calledBing = text(searchS \* search.resolvedWhat),
                    geoORDir = text(searchS \* search.geoORdir),
                    listingsCategoriesTiersMainListsAuxLists = {
                      val allCategories = event \* searchSection \* search.listingsCategoriesTiersMainListsAuxLists.base \* search.listingsCategoriesTiersMainListsAuxLists.category.base
                      rawevent.ListingsCategoriesTiersMainListsAuxLists(category =
                        allCategories.foldLeft(Set[rawevent.ListingCategory]()) { (currentSet, aCategory) =>
                          val categoryId = text(aCategory \* search.listingsCategoriesTiersMainListsAuxLists.category.id)
                          currentSet + rawevent.ListingCategory(id = categoryId)
                        })
                    },
                    matchedGeo = text(searchS \* search.matchedGeo),
                    allListingsTypesMainLists = text(searchS \* search.allListingsTypesMainLists),
                    relatedListingsReturned = text(searchS \* search.relatedListingsReturned),
                    directoriesReturned = text(searchS \* search.directoriesReturned),
                    allHeadings = {
                      rawevent.SearchHeadings(headings =
                        (event \* searchSection \* allHeadingsSection \* headingSection).
                          foldLeft(Set[rawevent.SearchHeading]()) { (currentSet, aHeading) =>
                            val headingName = aHeading \* heading.name
                            val headingCategory = aHeading \* heading.category
                            currentSet + rawevent.SearchHeading(text(headingName), text(headingCategory))
                          })
                    },
                    searchtype = text(searchS \* search.searchtype),
                    resultPage = text(searchS \* search.resultPage),
                    resultPerPage = text(searchS \* search.resultPerPage),
                    latitude = text(searchS \* search.latitude),
                    longitude = text(searchS \* search.longitude),
                    radius_1 = text(searchS \* search.radius_1),
                    radius_2 = text(searchS \* search.radius_2),
                    merchants = {
                      val allMerchants = event \* searchSection \* merchantsSection
                      allMerchants.foldLeft(Set[rawevent.Merchant]()) { (currentSet, aMerchant) =>
                        currentSet + rawevent.Merchant(
                          id = text(aMerchant.\.*@(merchants.id)),
                          zone = text(aMerchant.\.*@(merchants.zone)),
                          latitude = text(aMerchant.\.*@(merchants.latitude)),
                          longitude = text(aMerchant.\.*@(merchants.longitude)),
                          distance = text(aMerchant.\.*@(merchants.distance)),
                          ranking = text(aMerchant \* merchants.ranking),
                          isListingRelevant = text(aMerchant \* merchants.isListingRelevant),
                          entry = rawevent.MerchantEntry(
                            headings = {
                              val allHeadings = aMerchant \* merchants.entry.base \* merchants.entry.headings.base
                              allHeadings.foldLeft(Set[rawevent.EntryHeading]()) { (r, c) =>
                                r + rawevent.EntryHeading(isRelevant = text(c.\.*@(merchants.entry.headings.isRelevant)), categories = text(c \* merchants.entry.headings.categories))
                              }
                            },
                            directories = {
                              val allHeadings = aMerchant \* merchants.entry.base \* merchants.entry.directories.base
                              allHeadings.foldLeft(Set[rawevent.EntryDirectories]()) { (r, c) => r + rawevent.EntryDirectories(text(c \* merchants.entry.directories.channel1)) }
                            },
                            listingType = text(aMerchant \* merchants.entry.base \* merchants.entry.listingType)))
                      }
                    },
                    searchAnalysis = rawevent.SearchAnalysis(
                      fuzzy = text(searchS \* searchAnalysisSection \* search.searchAnalysis.fuzzy),
                      geoExpanded = text(searchS \* searchAnalysisSection \* search.searchAnalysis.geoExpanded),
                      businessName = text(searchS \* searchAnalysisSection \* search.searchAnalysis.businessName))))

                }
              }
              searchInfo
            },
            searchAnalytics = {
              rawevent.SearchAnalytics(entries = {
                val allSearchAnalyticsEntries = event \* searchAnalyticsSection \* searchAnalyticsEntry
                allSearchAnalyticsEntries.foldLeft(Set[rawevent.SearchAnalyticsEntry]()) { (currentSet, anEntry) =>
                  val key = anEntry \* entry.key
                  val value = anEntry \* entry.value
                  currentSet + rawevent.SearchAnalyticsEntry(text(key), text(value))
                }
              })
            })
        //
        ops.cumulate(ops.validateFromRaw(theRawEvent))(acc)
      }

    }

  }

}
