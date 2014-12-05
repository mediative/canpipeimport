package canpipe.parser.spark

import canpipe.CanpipeFileName
import canpipe.parser.{ RejectRule, FilterRule }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // Implicit conversions. See p. 5 of http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-2-amp-camp-2012-standalone-programs.pdf
import canpipe.parser.spark.{ Parser => SparkParser }
import spark.util.wrapper.HDFSFileName
import spark.util.xml.FileStructure
import spark.util.{ Base => SparkUtil }
import util.{ Base => BaseUtil }
import org.apache.hadoop.fs.Path

object RunParser {

  private val FILENAMELABEL = "hdfsfilename"
  def parseArgs(list: List[String]): Map[String, String] = {
    def loop(map: Map[String, String], list: List[String]): Map[String, String] = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--hdfsfilename" :: value :: tail =>
          loop(map + (FILENAMELABEL -> value), tail)
        case l =>
          println(s"Unknown ${if (isSwitch(l.head)) "switch" else "value"} " + l.head)
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
          println(s"Trying to mv from ${srcFileName} to ${dstFileName}")
          if (SparkUtil.HDFS.mv(srcFileName, dstFileName)) pairsFailing
          else pairsFailing + Tuple2(srcFileName, dstFileName)
      }
    pairsFailing.foreach {
      case (srcFileName, dstFileName) =>
        println(s"Impossible to mv from '${srcFileName}' to '${dstFileName}")
    }
    (pairsFailing.size == 0)
  }

  private[spark] def synchronizeMainFolderAndCleanSource(mainFolder: String, sourceFolder: String): Boolean = {
    if (synchronizeMainFolder(mainFolder, sourceFolder)) {
      SparkUtil.HDFS.rm(sourceFolder)
    } else {
      println(s"Something exploded while synchronizing '${mainFolder}' with info from '${sourceFolder}'")
      false
    }
  }

  private[spark] def saveRDDAsParquetAndCleanUp(
    sqlContext: org.apache.spark.sql.SQLContext,
    thisRDD: org.apache.spark.sql.SchemaRDD,
    workingDir: String,
    prefixOfFile: String,
    dirToSynchronize: String): Boolean = {
    import sqlContext._

    def sanityCheckParquetGeneration(whereWasItSaved: String): Boolean = {
      // Parquet files are self-describing so the schema is preserved.
      val rddFromParquetFile = sqlContext.parquetFile(whereWasItSaved)
      //Parquet files can also be registered as tables and then used in SQL statements.
      val tableName = s"${prefixOfFile}.table"
      rddFromParquetFile.registerAsTable(tableName)
      val allRowsInTable = sql(s"SELECT * FROM ${tableName}")
      val howManyRowsInTable = allRowsInTable.count()
      val howManyEntriesInRDD = thisRDD.count()
      //
      if (howManyEntriesInRDD != howManyRowsInTable) {
        // TODO: change this to 'error'
        println(s"Something wrong ==> there are ${howManyEntriesInRDD} in RDD and ${howManyRowsInTable} in resulting table")
      } else {
        // TODO: change this to 'info'
        println(s"Just successfully saved ${howManyEntriesInRDD} entries in Parquet file ${whereWasItSaved}")
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
    } else
      false
  }

  // TODO: replace all 'println' by writing to Spark logs
  def main(args: Array[String]) {
    import canpipe._
    val argsParsed = parseArgs(args.toList)
    if (!argsParsed.contains(FILENAMELABEL)) {
      println("Usage: RunParser [--hdfsfilename filename]")
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
      import sqlContext._
      // TODO: clean syntax
      val rddTF = sc.textFile(hdfsFileName)
      val fs = new FileStructure("root", rddTF)
      val tables = myParser.parse(fs)
      val fileNameNoDir = hdfsFileName.split("/").reverse.head
      val cleanedSrcFileName = fileNameNoDir.replace(" ", "").replace("-", "")
      // event table
      val events = tables.map(_.eventOpt).flatMap(identity(_))
      val (timeToSaveEvents, eventProperlySaved) =
        util.Base.timeInMs {
          saveRDDAsParquetAndCleanUp(
            sqlContext,
            thisRDD = events,
            workingDir = globalConf.eventsFolders.workingTmp,
            prefixOfFile = cleanedSrcFileName,
            dirToSynchronize = globalConf.eventsFolders.output)
        }
      if (!eventProperlySaved) {
        println(s"Event *NOT PROPERLY SAVED*, aborting this non-sense right now!!!")
      } else {
        // headings table
        val headings = tables.map(_.headings).flatMap(identity(_))
        val (timeToSaveHeadings, headingsProperlySaved) =
          util.Base.timeInMs {
            saveRDDAsParquetAndCleanUp(
              sqlContext,
              thisRDD = headings,
              workingDir = globalConf.headingsFolders.workingTmp,
              prefixOfFile = s"${cleanedSrcFileName}.headings",
              dirToSynchronize = globalConf.headingsFolders.output)
          }
        // directories table
        val directories = tables.map(_.directories).flatMap(identity(_))
        val (timeToSaveDirectories, directoriesProperlySaved) =
          util.Base.timeInMs {
            saveRDDAsParquetAndCleanUp(
              sqlContext,
              thisRDD = directories,
              workingDir = globalConf.directoriesFolders.workingTmp,
              prefixOfFile = s"${cleanedSrcFileName}.directories",
              dirToSynchronize = globalConf.directoriesFolders.output)
          }
        println(s"Saving EVENTS took ${timeToSaveEvents} ms. (added to location = ${globalConf.eventsFolders.output})")
        if (headingsProperlySaved)
          println(s"HEADINGS saved in ${timeToSaveHeadings} ms. (added to location = ${globalConf.headingsFolders.output})")
        else
          println("HEADINGS **NOT SAVED**")
        if (directoriesProperlySaved)
          println(s"DIRECTORIES saved in ${timeToSaveDirectories} ms. (added to location = ${globalConf.directoriesFolders.output})")
        else
          println("DIRECTORIES **NOT SAVED**")
      }
    }

  }

}
