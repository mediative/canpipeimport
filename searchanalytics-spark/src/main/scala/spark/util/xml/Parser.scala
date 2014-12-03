package spark.util.xml

import org.apache.spark.rdd.RDD

import scala.util.control.Exception._

object Parser {
  def parse(stringSet: RDD[String]): RDD[xml.Elem] = {
    stringSet.flatMap(s => catching(classOf[Exception]).opt { xml.XML.loadString(s) })
  }
}
