package util

/**
 * Represents an association between 2 tables when schema is normalized
 */
trait TwoTableAssociation[T1, T2] {
  def fk1: T1
  def fk2: T2
}

