package util.xml

import scala.xml.pull.{ EvElemEnd, EvText, EvElemStart, XMLEventReader }

object Parser {
  /*

  private def parseAttributes(attrsMap: Map[String, String], currentXPath: XPath): Map[String, List[String]] = { // TODO: make it clear (for the compiler) that the type returned here is the same as the one returned on the 'parse'
    attrsMap.foldLeft(Map.empty[String, List[String]]) {
      case (currentResultMap, (attrName, attrValue)) =>
        val attrLabel = "@" + attrName
        val fieldLabel = XPath.add(currentXPath, attrLabel).asString
        // logger.info(s"\t **** Found [${fieldLabel}] ==> '${label}' metadata field '${attrName}' = '${attrValue}'")
        val aMap = currentResultMap + (fieldLabel -> (currentResultMap.getOrElse(fieldLabel, List.empty) ++ List(attrValue)))
        aMap
    }
  }

  sealed trait Status[T] {
    def info: T
  }

  sealed trait EmptyStatus extends Status[Unit] {
    val info = _
  }

  case class Started(info: XPath) extends Status[XPath]
  case class Ended(info: XPath) extends Status[XPath]
  case class BuildingInfo(info: String) extends Status[String]

  def parse(xml: XMLEventReader): Set[Map[String, List[String]]] = {



    val anXMLAsString = "<root><event id=\"e1\"><allHeadings><heading>1</heading><heading>2</heading></allHeadings></event></root>"
    val anXML = scala.xml.XML.loadString(anXMLAsString)
    val xx = anXML \ "event"
    // res57: scala.xml.NodeSeq = NodeSeq(<event id="e1"><allHeadings><heading>1</heading><heading>2</heading></allHeadings></event>)

    val xx2 = anXML \ "event" \ "@id"
    // res58: scala.xml.NodeSeq = e1

    xx.

    /**
     *
     * @param comingFromXPathAsReverseList
     * @param totalResultMap
     * @return a Set, where each element is an Impression Event, coded like this:
     *         fieldLabel -> {values this label takes}
     */
    def loop(sourceXPath: XPath, status: Status, resultMap: Map[String, List[String]], totalResultMap: Set[Map[String, List[String]]]): Set[Map[String, List[String]]] = {
      if (xml.hasNext) {
        xml.next match {
          case EvElemStart(_, label, attrs, _) =>
            val currentXPath = XPath.add(sourceXPath, label)
            val eventFields = {
              // parse attributes of the event:
              val attrsMap = attrs.asAttrMap
              val eventMap = parseEvent(xml, currentXPath, eventIdOpt = Some(idNameAndValue._2))
              eventMap ++ parseAttributes(attrsMap, currentXPath)
            }
            // println(s"totalResultMap has size ${totalResultMap.size}")
            loop(sourceXPath, Started(currentXPath), eventFields, totalResultMap)
          case EvText(text) =>
            status match {
              case BuildingInfo(soFar) => loop(sourceXPath, BuildingInfo(s"${soFar}&${text}"), resultMap, totalResultMap)
              case Started(_) => loop(sourceXPath, BuildingInfo(text), resultMap, totalResultMap)
              case Ended(_) => // syntax error in XML. Just ignore it:
                // TODO: report it
                loop(sourceXPath, status, resultMap, totalResultMap)
            }
          case EvElemEnd(_, label) =>
            val fieldLabel = sourceXPath.asString
            val ff = (fieldLabel -> (totalResultMap.getOrElse(fieldLabel, List.empty) ++ List(fieldValue)))
            loop(XPath.removeLast(sourceXPath), status = Ended(sourceXPath), totalResultMap + (fieldLabel -> (totalResultMap.getOrElse(fieldLabel, List.empty) ++ List(fieldValue))))

            if (label == "Event") {
              // end of the parsing of the event
              // println(s"end of the parsing of the event. Result Map has ${totalResultMap.size} entries")
              totalResultMap
            } else {
              loop(eventId, XPath.removeLast(sourceXPath), totalResultMap)
            }
          case _ => loop(sourceXPath, totalResultMap)
        }
      } else {
        totalResultMap
      }
    }

    loop(XPath.fromRoot(), totalResultMap = Set.empty)
  }

*/
}
