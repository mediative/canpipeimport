package canpipe

import canpipe.xml.{ Elem => CanpipeXMLElem }

/**
 * TODO: doc.
 */
case class Tables(anXMLNode: CanpipeXMLElem) {

  val (events: List[EventDetail], headings: List[EventsHeadingsAssociation], directories: List[EventsDirectoriesAssociation]) = {
    (EventDetail(anXMLNode).flatMap(EventDetail.sanityCheck), List.empty[EventsHeadingsAssociation], List.empty[EventsDirectoriesAssociation])
  }

}

