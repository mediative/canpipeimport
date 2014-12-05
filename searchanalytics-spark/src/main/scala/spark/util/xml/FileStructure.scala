package spark.util.xml

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.util.wrapper.HDFSFileName

trait Structure extends Serializable {
  def starter: String
  def lines: RDD[String]
}

/**
 * Abstracts the structure of a file that has an XML inside.
 */
case class FileStructure(starter: String, rdd: RDD[String]) extends Structure {

  private def getLines(wrapped: Boolean): RDD[String] = {
    val rdd1 = rdd.filter(!_.endsWith(starter + ">"))
    if (wrapped) rdd1.map(line => s"<${starter}>${line}</${starter}>")
    else rdd1
  }

  def lines: RDD[String] = getLines(wrapped = true)

}
