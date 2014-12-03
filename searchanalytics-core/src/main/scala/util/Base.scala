package util

// import com.typesafe.scalalogging.Logging
// import com.typesafe.scalalogging.slf4j.StrictLogging

object Base { // extends Logging {

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
    try { f(param) } finally { param.close() }

  object String {

    def generateRandom(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString("")

  }

}
