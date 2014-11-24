package canpipe.parser

import util.Base.XML.XPath

import scala.io.Source
import scala.xml.pull.{ EvText, EvElemEnd, EvElemStart, XMLEventReader }

object Base {
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

  case class headingEntry(event_id: String, heading: String, category: String)
  object HeadingReadingStatus extends Enumeration {
    type HeadingReadingStatus = Value
    val NONE, READING_HEADING, READING_NAME, READING_CATEGORY = Value
  }
  import HeadingReadingStatus._

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
    private[parser] def processText(xml: XMLEventReader, partsOfTextInt: List[String]): (Boolean, String) = {
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
    private[parser] def processAllHeadingsForEvent(xml: XMLEventReader, eventId: String, headings: List[headingEntry]): List[headingEntry] = {
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
    def parseEvent(xml: XMLEventReader,
                   startXPath: XPath, eventIdOpt: Option[String]): (Map[String, List[String]], List[headingEntry]) = {

      // println(s"parseEvent(${eventId}}): startXPath: ${startXPath.asString}")
      def loop(eventId: String, sourceXPath: XPath, resultMap: Map[String, List[String]],
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
                  loop(eventId, sourceXPath, resultMap, processAllHeadingsForEvent(xml, eventId, headings))
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
                  loop(eventId, currentXPath, newCurrentMap,
                    headings)
              }
            }
            case EvElemEnd(_, label) =>
              if (label == "Event") {
                // end of the parsing of the event
                // println(s"end of the parsing of the event. Result Map has ${resultMap.size} entries")
                (resultMap, headings)
              } else {
                loop(eventId, XPath.removeLast(sourceXPath), resultMap, headings)
              }
            case EvText(text) =>
              // logger.info(s"TEXT ===> ${text}")
              val (reachedEndOfEvent, fieldValue) = processText(xml, List(text))
              if (reachedEndOfEvent)
                (resultMap, headings)
              else {
                loop(eventId, XPath.removeLast(sourceXPath),
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
              loop(eventId, sourceXPath, resultMap, headings)
            }
          }
        } else
          (resultMap, headings)
      }

      // TODO: if eventId is undefined, then I must be parsing an event from the beginning.
      // insert code here to get eventId
      val optResult =
        eventIdOpt match {
          case Some(eventId) => Some(eventId, Map.empty[String, List[String]], List.empty[headingEntry])
          case None =>
            if (xml.hasNext) {
              xml.next match {
                case EvElemStart(_, label, attrs, _) if (label == "Event") =>
                  // parse attributes of the event:
                  val attrsMap = attrs.asAttrMap
                  attrsMap.find { case (attrName, _) => attrName == "id" } match {
                    case None =>
                      println("No [id] for event!!! Ignoring.")
                      None
                    case Some(idNameAndValue) =>
                      val currentXPath = XPath.add(startXPath, label)
                      val eventid = idNameAndValue._2
                      Some(eventid, parseAttributes(attrsMap, currentXPath), List.empty[headingEntry])
                  }
                case _ =>
                  println("THIS IS NOT AN EVENT")
                  None
              }
            } else {
              None
            }
        }
      optResult.map {
        case (theEventId, theResultMap, theHeadings1) =>
          val (fieldsInImpression, theHeadings) = loop(theEventId, startXPath, resultMap = theResultMap, headings = theHeadings1)
          (fieldsInImpression, theHeadings)
      }.getOrElse((Map.empty[String, List[String]], List.empty[headingEntry]))
    }

    private def parseAttributes(attrsMap: Map[String, String], currentXPath: XPath): Map[String, List[String]] = {
      attrsMap.foldLeft(Map.empty[String, List[String]]) {
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
      }
    }

    // reference: http://stackoverflow.com/questions/13184212/parsing-very-large-xml-lazily
    def parseFromResources(fileName: String, filterRules: Set[FilterRule]): (Set[Map[String, scala.List[String]]], List[headingEntry]) = {

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
              case EvElemStart(_, label, attrs, _) =>
                if (label == "Event") {
                  val (eventFields, eventHeadings) = {
                    // parse attributes of the event:
                    val attrsMap = attrs.asAttrMap
                    attrsMap.find { case (attrName, _) => attrName == "id" } match {
                      case None =>
                        println("No [id] for event!!! Ignoring.")
                        // logger.info("No [id] for impression!!! Ignoring.")
                        (Map.empty[String, scala.List[String]], headings)
                      case Some(idNameAndValue) =>
                        val currentXPath = XPath.add(sourceXPath, label)
                        val (eventMap, eventHeadings) = parseEvent(xml, currentXPath, eventIdOpt = Some(idNameAndValue._2))
                        (eventMap ++ parseAttributes(attrsMap, currentXPath), eventHeadings ++ headings)
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

        loop(XPath.fromRoot(), resultMap = Set.empty, headings = List.empty)
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

