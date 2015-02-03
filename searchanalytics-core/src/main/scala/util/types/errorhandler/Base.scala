package util.errorhandler

/**
 * Generic 'type' error handler.
 */
object Base {

  trait ErrorSym[T] {
    def handle: Option[T] => Either[String, T]
  }
  trait GenericHandler[T] extends ErrorSym[T] {
    protected def typeAsString: String
    def handle = optLong => optLong match {
      case None => Left(s"Value is not a ${typeAsString}")
      case Some(v) => Right(v)
    }
    def apply(optLong: Option[T]): Either[String, T] = handle(optLong)
  }
  implicit object IntHandler extends GenericHandler[Int] {
    protected def typeAsString: String = "Int"
  }
  implicit object LongHandler extends GenericHandler[Long] {
    protected def typeAsString: String = "Long"
  }
  implicit object BooleanHandler extends GenericHandler[Boolean] {
    protected def typeAsString: String = "Boolean"
  }
  implicit object DoubleHandler extends GenericHandler[Double] {
    protected def typeAsString: String = "Double"
  }

}
