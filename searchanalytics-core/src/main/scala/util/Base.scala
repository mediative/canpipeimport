package util

import scalaz.{ Writer }

object Base {

  def getCurrentTimeStamp: java.sql.Timestamp = {
    new java.sql.Timestamp(java.util.Calendar.getInstance().getTime.getTime)
  }

  private def measureTime[A](getTime: () => Long)(block: => A): scalaz.Writer[Long, A] = {
    lazy val (executionTime, result) = {
      val t0 = getTime()
      val result = block
      (getTime() - t0, result)
    }
    Writer(executionTime, result)
  }

  def timeInMs[R](block: => R): Writer[Long, R] = measureTime(System.currentTimeMillis)(block)
  def timeInNanoSecs[R](block: => R): Writer[Long, R] = measureTime(System.nanoTime)(block)

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
