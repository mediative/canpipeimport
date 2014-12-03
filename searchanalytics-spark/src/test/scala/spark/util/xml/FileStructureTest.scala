package spark.util.xml

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import spark.util.Base.HDFS
import spark.util.wrapper.HDFSFileName
import util.Base.using

import scala.util.control.Exception._

class FileStructureTest extends FlatSpec with BeforeAndAfter {

  val nonExistentFileName = util.Base.String.generateRandom(10)

  before {
    assert(!HDFS.fileExists(nonExistentFileName))
  }

  after {

  }

  "Creating an XML file structure" should "produce empty lines for empty file" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    try {
      // TODO: clean syntax
      val rdd = sc.textFile(nonExistentFileName)
      val fs = new FileStructure("root", rdd) // HDFSFileName(nonExistentFileName))
      val rddWithLines = fs.lines
      val theCount = catching(classOf[Exception]).opt { rddWithLines.count() }.getOrElse(0L)
      assert(theCount == 0)
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }

  }

  it should "work if I save and then read" in {
    val eventsXML =
      <root>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb840" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
        <Event id="3a3637cd-21f9-40c9-9e1d-b44890ffb841" timestamp="2014-09-30T12:00:00.054-04:00" site="ypg" siteLanguage="EN" eventType="impression" isMap="false" typeOfLog="impression"></Event>
      </root>

    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)
    val fileNameCreated = s"${util.Base.String.generateRandom(10)}.xml"

    HDFS.writeToFile(fileName = fileNameCreated, eventsXML.toString())
    try {
      // TODO: clean syntax
      val rdd = sc.textFile(fileNameCreated)
      val fs = new FileStructure("root", rdd)
      val rddWithLines = fs.lines
      val theCount = rddWithLines.count()
      assert(theCount == 2)
    } finally {
      HDFS.rm(fileNameCreated)
    }
  }

}
