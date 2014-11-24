package canpipe.parser

// TODO: put this in proper location
sealed trait FilterRule {
  def name: String
  def values: Set[String]
}

case class AcceptOnlyRule(name: String, values: Set[String]) extends FilterRule
case class RejectRule(name: String, values: Set[String]) extends FilterRule

