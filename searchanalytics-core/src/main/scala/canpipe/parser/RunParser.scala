package canpipe.parser

import canpipe.parser.Base.{ FieldImportance, CanPipeParser }
import canpipe.parser.Base.FieldImportance._
import canpipe.parser.Base.fieldsDef

object RunParser {
  def main(args: Array[String]) {
    // // ("/sample.50.xml"))) // ("/sample1Impression.xml"))) //  // ("/analytics.log.2014-06-01-15")))
    def timeInMs[R](block: => R): (Long, R) = {
      val t0 = System.currentTimeMillis()
      val result = block // call-by-name
      val t1 = System.currentTimeMillis()
      ((t1 - t0), result)
    }
    // define filtering rules for this XML:
    val filterRules: Set[FilterRule] = Set.empty // TODO
    //
    val fileNameToParseFromResources = "sample.50.xml"
    val myParser = new CanPipeParser(filterRules)
    println("COMPUTATION STARTED")
    val (timeItTook, setOfImpressions) = timeInMs(myParser.parseFromResources(fileNameToParseFromResources))
    println(s"Computation finished in ${timeItTook} ms.")

    val listOfMissing: List[(FieldImportance.Value, Map[String, Long])] =
      List(Must, Should).map { importanceLevel =>
        (importanceLevel,
          setOfImpressions.foldLeft(Map.empty[String, Long]) {
            case (mapOfMissing, mapOfResult) =>
              (fieldsDef.filter { case (_, importance) => importance == importanceLevel }.keySet -- mapOfResult.keySet).foldLeft(mapOfMissing) { (theMap, aFieldName) =>
                theMap + (aFieldName -> (theMap.getOrElse(aFieldName, 0L) + 1))
              }
          })
      }
    // report of SHOULD/MUST fields that are missing:
    println(s"**************  Report of fields that are missing, on the sampling of ${setOfImpressions.size} events **************")
    listOfMissing.foreach {
      case (importanceLevel, report) =>
        println(s"**************  Report of ${importanceLevel} fields that are missing  **************")
        report.foreach {
          case (aFieldName, howManyTimesMissing) =>
            println(s"'${aFieldName}', missed ${howManyTimesMissing} times")
        }
    }

    /*
    println(s"'${setOfImpressions.size} IMPRESSION events parsed  ==> ")
    setOfImpressions.foreach { mapOfResult =>
      // display all:
      println("**************  Report of ALL fields  **************")
      mapOfResult.foreach {
        case (fieldName, listOfValues) =>
          println(s"\t 'field = ${fieldName}', value = ${listOfValues.mkString(start = "{", sep = ",", end = "}")}")
      }
      // val eD = eventDetail(mapOfResult)
      // logger.info(s"${eD.toString}")
    }
    */
  }

}
