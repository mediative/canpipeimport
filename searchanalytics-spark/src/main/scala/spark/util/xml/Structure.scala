package spark.util.xml

import org.apache.spark.rdd.RDD

/**
 * Encapsulates the basic structure of an XML: it has "lines" and it has a "starter" (ie, a "root") symbol
 * For example, the XML
 * <pre>
 *   {@code
 *   <xml>
 *     <line>line1</line>
 *     <line>line2</line>
 *   </xml>
 *   }
 * </pre>
 * would be represented
 * by starter = 'xml', lines:RDD[String] = RDD(line1, line2)
 * In other words, the lines containing the starter are not containing in the list.
 */
trait Structure {
  def starter: String
  def lines: RDD[String]
  def wrappedLines: RDD[String] = lines.map(line => s"<${starter}>${line}</${starter}>")
}

