package canpipe.parser.spark

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import spark.util.Base.HDFS
import util.Base._

class RunParserTest extends FlatSpec with BeforeAndAfter {

  private def writeASampleFile(fileName: String): Boolean = {
    try {
      val output = FileSystem.get(new Configuration()).create(new Path(fileName))
      using(new PrintWriter(output)) { writer =>
        writer.write("lalala")
      }
      true
    } catch {
      case e: Exception => {
        println(s"Error: ${e.getMessage}")
        false
      }
    }
  }

  before {
  }

  after {

  }

  "synchronizeMainFolderMvFiles" should "simply work" in {
    val sourceDirName = "tmptmp"
    val mainDirName = "maindir"
    if (!HDFS.directoryExists(sourceDirName)) {
      info(s"Directory '${sourceDirName}' being created")
      withClue(s"Impossible to create directory '${sourceDirName}'") { assert(HDFS.getFileSystem.mkdirs(new Path(sourceDirName))) }
      withClue(s"Impossible to create file '_metadata'") { assert(writeASampleFile(fileName = s"${sourceDirName}/_metadata")) }
    }
    info(s"ls on directory '${sourceDirName}' ==> ")
    HDFS.ls(sourceDirName, recursive = false).foreach(n => info(s"\t ${n}"))
    val s = RunParser.synchronizeMainFolderMvFiles(mainFolder = mainDirName, sourceFolder = sourceDirName)
    withClue("No files to synchronize") { assert(s.size == 1) }
    assert(s.head._1.endsWith("_metadata"))
    assert(s.head._2.endsWith("_metadata"))
    withClue(s"Impossible to remove directory '${sourceDirName}'") { assert(HDFS.rm(sourceDirName)) }
    withClue(s"Impossible to remove directory '${mainDirName}'") { assert(HDFS.rm(mainDirName)) }
  }

}
