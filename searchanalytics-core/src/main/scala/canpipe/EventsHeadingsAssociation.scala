package canpipe

import util.TwoTableAssociation

case class EventsHeadingsAssociation(eventId: String, headingId: Long, category: String) extends TwoTableAssociation[String, Long] {
  val fk1 = eventId
  val fk2 = headingId
}
