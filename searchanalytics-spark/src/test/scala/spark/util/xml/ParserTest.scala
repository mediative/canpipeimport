package spark.util.xml

import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import spark.util.Base.HDFS

class ParserTest extends FlatSpec with BeforeAndAfter {

  val nonExistentFileName = util.Base.String.generateRandom(10)

  before {
    assert(!HDFS.fileExists(nonExistentFileName))
  }

  after {

  }

  "Parsing a Set of String" should "yield an empty RDD if Set is empty" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    try {
      assert(Parser.parse(stringSet = sc.parallelize(List.empty[String])).count() == 0)
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "yield an empty RDD if lines are not in XML syntax" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    try {
      assert(Parser.parse(stringSet = sc.parallelize(List(""))).count() == 0)
      assert(Parser.parse(stringSet = sc.parallelize(List("hello"))).count() == 0)
      assert(Parser.parse(stringSet = sc.parallelize(List("<b>hello</a>"))).count() == 0)
      assert(Parser.parse(stringSet = sc.parallelize(List("<b>hello</a>", "<b>hello</a>"))).count() == 0)
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "yield a proper-sized RDD when lines are in proper XML syntax" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    try {
      assert(Parser.parse(stringSet = sc.parallelize(List("<b>hello</b>"))).count() == 1)
      assert(Parser.parse(stringSet = sc.parallelize(List("<b>hello</b>", "<a>hello</a>"))).count() == 2)
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

  it should "yield an RDD with proper tags when lines are correct" in {
    val testName = "my test"
    val sc = new SparkContext("local[4]", testName)

    try {
      val rdd = Parser.parse(stringSet = sc.parallelize(
        List(
          "<b>B</b>", // syntax OK, but no proper starter
          "<xml><b>B</b></xml>",
          "<xml><a>A</a></xml>")))
      assert(rdd.count() == 3)
      val a = rdd.collect()
      a.zipWithIndex.foreach {
        case (node, i) =>
          i match {
            case 0 => withClue("First node failed") { assert((node \ "b").text == "") }
            case 1 => withClue("Second node failed") { assert((node \ "b").text == "B") }
            case 2 => withClue("Third node failed") { assert((node \ "a").text == "A") }
            case n => withClue(s"index = ${n}?????") { assert(false) }
          }

      }
    } finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

}
