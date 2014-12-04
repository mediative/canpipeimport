package canpipe.parser.spark

import canpipe.CanpipeFileName
import canpipe.parser.{ RejectRule, FilterRule }
import org.apache.spark.SparkContext
import canpipe.parser.spark.{ Parser => SparkParser }
import org.apache.spark.rdd.RDD
import spark.util.xml.XMLPiecePerLine
import spark.util.{ Base => SparkUtil }
import util.{ Base => BaseUtil, Logging }
import org.apache.hadoop.fs.Path

object RunParser extends Logging {

  private val FILENAMELABEL = "hdfsfilename"
  def parseArgs(list: List[String]): Map[String, String] = {
    def loop(map: Map[String, String], list: List[String]): Map[String, String] = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--hdfsfilename" :: value :: tail =>
          loop(map + (FILENAMELABEL -> value), tail)
        case l =>
          logger.info(s"Unknown ${if (isSwitch(l.head)) "switch" else "value"} " + l.head)
          loop(map, l.tail)
      }
    }
    loop(Map.empty, list)
  }

  /**
   *
   * @param mainFolder
   * @param sourceFolder
   * @return
   */
  private[spark] def synchronizeMainFolderMvFiles(mainFolder: String, sourceFolder: String): Set[(String, String)] = {
    val currentTimestamp = BaseUtil.getCurrentTimeStamp.toString.replace(" ", ".").replace(":", ".")
    SparkUtil.HDFS.ls(sourceFolder, recursive = false).foldLeft(Set.empty[(String, String)]) {
      case (resultSet, fileName) =>
        val fileNameNoDir = fileName.split(Path.SEPARATOR).reverse.head
        fileNameNoDir match {
          case "_metadata" =>
            if (!SparkUtil.HDFS.fileExists(s"${mainFolder}${Path.SEPARATOR}_metadata"))
              resultSet + Tuple2(s"${sourceFolder}${Path.SEPARATOR}_metadata", s"${mainFolder}${Path.SEPARATOR}_metadata")
            else
              resultSet
          case n if (n.startsWith("part")) =>
            resultSet + Tuple2(fileName, s"${mainFolder}${Path.SEPARATOR}${currentTimestamp}-${n}")
          case _ => resultSet
        }
    }
  }

  /**
   *
   * @param mainFolder
   * @param sourceFolder
   * @return
   */
  private[spark] def synchronizeMainFolder(mainFolder: String, sourceFolder: String): Boolean = {
    val pairsFailing =
      synchronizeMainFolderMvFiles(mainFolder, sourceFolder).foldLeft(Set.empty[(String, String)]) {
        case (pairsFailing, (srcFileName, dstFileName)) =>
          logger.debug(s"Trying to mv from ${srcFileName} to ${dstFileName}")
          if (SparkUtil.HDFS.mv(srcFileName, dstFileName)) pairsFailing
          else pairsFailing + Tuple2(srcFileName, dstFileName)
      }
    pairsFailing.foreach {
      case (srcFileName, dstFileName) =>
        logger.error(s"Impossible to mv from '${srcFileName}' to '${dstFileName}")
    }
    (pairsFailing.size == 0)
  }

  private[spark] def synchronizeMainFolderAndCleanSource(mainFolder: String, sourceFolder: String): Boolean = {
    if (synchronizeMainFolder(mainFolder, sourceFolder)) {
      SparkUtil.HDFS.rm(sourceFolder)
    } else {
      logger.error(s"Something exploded while synchronizing '${mainFolder}' with info from '${sourceFolder}'")
      false
    }
  }

  private[spark] def saveRDDAsParquetAndCleanUp(sqlContext: org.apache.spark.sql.SQLContext, thisRDD: org.apache.spark.sql.SchemaRDD, workingDir: String, prefixOfFile: String, dirToSynchronize: String): Unit = {

    def sanityCheckParquetGeneration(whereWasItSaved: String): Boolean = {
      // Parquet files are self-describing so the schema is preserved.
      val rddFromParquetFile = sqlContext.parquetFile(whereWasItSaved)
      //Parquet files can also be registered as tables and then used in SQL statements.
      val tableName = s"${prefixOfFile}.table"
      rddFromParquetFile.registerAsTable(tableName)
      val allRowsInTable = sqlContext.sql(s"SELECT * FROM ${tableName}")
      val howManyRowsInTable = allRowsInTable.count()
      val howManyEntriesInRDD = thisRDD.count()
      //
      if (howManyEntriesInRDD != howManyRowsInTable) {
        logger.error(s"Something wrong ==> there are ${howManyEntriesInRDD} in RDD and ${howManyRowsInTable} in resulting table")
      } else {
        logger.info(s"Just successfully saved ${howManyEntriesInRDD} entries in Parquet file ${whereWasItSaved}")
      }
      (howManyEntriesInRDD == howManyRowsInTable)
    }

    val parquetFileName = s"${workingDir}/${prefixOfFile}.parquet"
    // clean it, if present:
    SparkUtil.HDFS.rm(parquetFileName)
    thisRDD.saveAsParquetFile(parquetFileName)
    // sanity check:
    if (sanityCheckParquetGeneration(whereWasItSaved = parquetFileName)) {
      synchronizeMainFolderAndCleanSource(mainFolder = dirToSynchronize, sourceFolder = parquetFileName)
    }
  }

  private[spark] def saveEventsRDDAsParquetAndCleanUp(sc: SparkContext, sqlContext: org.apache.spark.sql.SQLContext, eventsRDD: RDD[EventDetail], workingDir: String, prefixOfFile: String, dirToSynchronize: String): Unit = {
    def sanityCheckParquetGeneration(whereWasItSaved: String): Boolean = {
      // Parquet files are self-describing so the schema is preserved.
      val rddFromParquetFile = sqlContext.parquetFile(whereWasItSaved)
      //Parquet files can also be registered as tables and then used in SQL statements.
      val tableName = s"${prefixOfFile}.table"
      rddFromParquetFile.registerAsTable(tableName)
      val allRowsInTable = sqlContext.sql(s"SELECT * FROM ${tableName}")
      val howManyRowsInTable = allRowsInTable.count()
      val howManyEntriesInRDD = eventsRDD.count()
      //
      if (howManyEntriesInRDD != howManyRowsInTable) {
        logger.error(s"Something wrong ==> there are ${howManyEntriesInRDD} in RDD and ${howManyRowsInTable} in resulting table")
      } else {
        logger.info(s"Just successfully saved ${howManyEntriesInRDD} entries in Parquet file ${whereWasItSaved}")
      }
      (howManyEntriesInRDD == howManyRowsInTable)
    }

    val parquetFileName = s"${workingDir}/${prefixOfFile}.parquet"
    // clean it, if present:
    SparkUtil.HDFS.rm(parquetFileName)
    EventDetail.saveAsParquet(sc, parquetFileName, eventsRDD, force = true)
    // sanity check:
    if (sanityCheckParquetGeneration(whereWasItSaved = parquetFileName)) {
      synchronizeMainFolderAndCleanSource(mainFolder = dirToSynchronize, sourceFolder = parquetFileName)
    }
  }

  def main(args: Array[String]) {
    // TODO: put all these constants in a config file and/or read them from parameters in call
    val HDFS_ROOT_LOCATION = "/source/canpipe/parquet" // root hdfs directory where data will live, once generated
    val HDFS_WORKING_LOCATION = s"${HDFS_ROOT_LOCATION}/workingTmp"
    val HDFS_EVENTS_WORKING_LOCATION = s"${HDFS_WORKING_LOCATION}/events" // directory where events will live, once generated
    val HDFS_EVENTS_LOCATION = s"${HDFS_ROOT_LOCATION}/events" // directory where events will live, once generated
    val argsParsed = parseArgs(args.toList)
    if (!argsParsed.contains(FILENAMELABEL)) {
      logger.info("Usage: RunParser [--hdfsfilename filename]")
    } else {
      val hdfsFileName = argsParsed.get(FILENAMELABEL).get // 'get' will never fail, because of 'if'
      // define filtering rules for this XML:
      val filterRules: Set[FilterRule] = Set(RejectRule(name = "/root/Event/user/browser", values = Set("BOT")))
      /*
      TODO:
        We should import for the first load all events where “site” is not “api”, and if “site”=”api”, only the following UserIds:

        site	platform
        api	api-ypg-searchapp-windows8
        api	api-canpages-searchapp-mobileweb
        api	api-ypg-searchapp-iphone
        api	api-ypg-searchapp-android
        api	api-ypg-searchapp-mobileweb
        api	api-yahoo-serachapp-mobileweb
        api	api-ypg-searchapp-android-html5
        api	api-ypg-searchapp-blackberry-BB10
        api	api-ypg-searchapp-blackberry-OS67

        Here is the REGEX for the IPs to exclude as YPG internal traffic: "207\.236\.194\.|204\.11\.58\.|64\.114\.164\.131"
       */
      val myParser = new SparkParser(filterRules)
      //
      val sc = new SparkContext()
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.createSchemaRDD
      // TODO: clean syntax
      val rddF = sc.textFile(hdfsFileName)
      val allEvents = myParser.parse(new XMLPiecePerLine("root", rddF))
      val fileNameNoDir = hdfsFileName.split("/").reverse.head
      val cleanedFileName = fileNameNoDir.replace(" ", "").replace("-", "")
      val (timeToSave, _) =
        util.Base.timeInMs {
          saveEventsRDDAsParquetAndCleanUp(sc, sqlContext, allEvents, workingDir = HDFS_EVENTS_WORKING_LOCATION, prefixOfFile = cleanedFileName, dirToSynchronize = HDFS_EVENTS_LOCATION)
        }.run
      logger.info(s"Saving took ${timeToSave} ms.!!!")
    }

  }

}
