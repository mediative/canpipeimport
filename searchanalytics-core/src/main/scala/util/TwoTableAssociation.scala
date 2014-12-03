package util

/**
 * Represents an association between 2 tables when schema is normalized
 */
trait TwoTableAssociation {
  def fk1: Long
  def fk2: Long
}

