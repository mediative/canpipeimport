package canpipe

/**
 * Encapsulates the 2 types of directories we will work with destination files:
 * a working directory, temporal
 * the actual output directory
 */
case class DstFolders(workingTmp: String, output: String)

// TODO: read configuration fron a file.
class Configuration {

  /**
   * root hdfs directory where data will live, once generated
   */
  private def locationForDstFiles = "/source/canpipe/parquet"

  /**
   * A working directory where we can dump files to be processed
   */
  private def workingTmp = s"${locationForDstFiles}/workingTmp"

  val eventsFolders = DstFolders(workingTmp = s"${workingTmp}/events", output = s"${locationForDstFiles}/events")
  val headingsFolders = DstFolders(workingTmp = s"${workingTmp}/headings", output = s"${locationForDstFiles}/headings")
  val directoriesFolders = DstFolders(workingTmp = s"${workingTmp}/directories", output = s"${locationForDstFiles}/directories")

}
