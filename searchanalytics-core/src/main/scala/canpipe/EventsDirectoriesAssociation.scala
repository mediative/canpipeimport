package canpipe

import util.TwoTableAssociation

class EventsDirectoriesAssociation(eventId: Long, directoryId: Long) extends TwoTableAssociation {
  val fk1 = eventId
  val fk2 = directoryId
}

