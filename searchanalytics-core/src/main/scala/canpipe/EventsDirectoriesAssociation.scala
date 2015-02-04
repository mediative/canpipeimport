package canpipe

import util.TwoTableAssociation

case class EventsDirectoriesAssociation(eventId: String, directoryId: Long) extends TwoTableAssociation[String, Long] {
  val fk1 = eventId
  val fk2 = directoryId
}

