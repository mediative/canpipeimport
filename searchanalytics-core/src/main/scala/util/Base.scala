package util

// import com.typesafe.scalalogging.Logging
// import com.typesafe.scalalogging.slf4j.StrictLogging

object Base {
  // extends Logging {

  def getCurrentTimeStamp: java.sql.Timestamp = {
    new java.sql.Timestamp(java.util.Calendar.getInstance().getTime.getTime)
  }

  def timeInMs[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def timeInNanoSecs[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    ((t1 - t0), result)
  }

  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }

  object String {

    def generateRandom(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString("")
    def toBooleanOpt(s: String): Option[Boolean] = {
      s.trim.toUpperCase match {
        case "TRUE" | "1" | "T" | "TRUE" | "Y" | "YES" => Some(true)
        case "FALSE" | "0" | "F" | "FALSE" | "N" | "NO" => Some(false)
        case x if (x.isEmpty) => Some(false)
        case _ => None
      }
    }

  }

  /**
   * Ensures a list meets minimal size, filling it up, if needed, with a defautl value.
   * @param aList Reference (starting) list
   * @param n Size to be met
   * @param default Values the list will be filled with if not enough elements are present in initial list
   * @return a list with at least 'n' elements of type T.
   */
  def fillToMinimumSize[T](aList: List[T], n: Int, default: T): List[T] = {
    aList ++ List.tabulate(n - aList.length)(_ => default)
  }

}

