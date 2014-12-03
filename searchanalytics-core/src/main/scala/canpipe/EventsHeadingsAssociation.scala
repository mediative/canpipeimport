package canpipe

import util.TwoTableAssociation

case class EventsHeadingsAssociation(eventId: Long, headingId: Long) extends TwoTableAssociation {
  val fk1 = eventId
  val fk2 = headingId
}
