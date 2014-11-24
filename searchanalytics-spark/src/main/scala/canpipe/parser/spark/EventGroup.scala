package canpipe.parser.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait EventGroup {
  def eventsAsString: RDD[String]
}

case class EventGroupFromHDFSFile(sc: SparkContext, hdfsFileName: String) extends EventGroup {
  val eventsAsString: RDD[String] = sc.textFile(hdfsFileName).filter(!_.endsWith("root>"))
}

