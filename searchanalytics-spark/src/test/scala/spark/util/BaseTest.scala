package spark.util

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import spark.util.Base.HDFS
import util.Base.using

class BaseTest extends FlatSpec with BeforeAndAfter {

  // TODO: generate here a name that FOR SURE would make the directory non-existent
  // (eg, a VERY long word?)
  val nonExistentDirectoryName = "lalala"

  before {
    assert(!HDFS.directoryExists(nonExistentDirectoryName))
  }

  after {

  }

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

  "HDFS List of Files in Folder" should "be empty for a non-existent directory" in {
    Set(true, false) foreach { r => assert(HDFS.ls(nonExistentDirectoryName, recursive = r).isEmpty) }
  }

  it should "start with name of folder" in {
    Set(true, false) foreach { r =>
      HDFS.ls(nonExistentDirectoryName, recursive = r) foreach { fileName =>
        assert(fileName.startsWith(nonExistentDirectoryName))
      }
    }
  }

  it should "start with name of folder when folder is HERE" in {
    val directoryName = "."
    assert(HDFS.directoryExists(directoryName))
    Set(true, false) foreach { r =>
      HDFS.ls(directoryName, recursive = r) foreach { fileName =>
        assert(fileName.startsWith(directoryName))
      }
    }
  }

  "HDFS File Exists" should "reject stupid files" in {
    assert(!HDFS.fileExists("lalala"))
  }

  it should "see created files as FILES, not as a DIRECTORIES" in {
    val fileName = "luis.txt"
    withClue(s"Impossible to create HDFS file ${fileName}") { assert(writeASampleFile(fileName)) }
    assert(HDFS.fileExists(fileName))
    assert(!HDFS.directoryExists(fileName))
    withClue(s"Impossible to DELETE HDFS file ${fileName}") { assert(HDFS.rm(fileName)) }
    withClue(s"NOT PROPERLY CLEANED AFTER (${fileName})") { assert(!HDFS.fileExists(fileName)) }
  }

  "HDFS File mv" should "fail when source file does not exist" in {
    val stupidFileName = "lalala"
    assert(!HDFS.fileExists(stupidFileName))
    assert(!HDFS.mv(stupidFileName, "righthere.txt"))
  }

  it should "work when source file exists" in {
    val srcFileName = "luis.txt"
    val dstFileName = "anotherfile.txt"
    withClue(s"Impossible to create HDFS file ${srcFileName}") { assert(writeASampleFile(srcFileName)) }
    assert(HDFS.fileExists(srcFileName))
    assert(HDFS.mv(srcFileName, dstFileName))
    // source file should not exist anymore...
    withClue(s"${srcFileName} should not exist after a MOVE!") { assert(!HDFS.fileExists(srcFileName)) }
    // clean up dst file:
    withClue(s"Impossible to DELETE HDFS file ${dstFileName}") { assert(HDFS.rm(dstFileName)) }
    withClue(s"NOT PROPERLY CLEANED AFTER (${dstFileName})") { assert(!HDFS.fileExists(dstFileName)) }
  }

  "HDFS File rm" should "succeed when source file does not exist" in {
    val stupidFileName = "lalala"
    withClue(s"File '${stupidFileName}' exists, test invalid!") { assert(!HDFS.fileExists(stupidFileName)) }
    assert(HDFS.rm(stupidFileName))
  }

  // NB: putting these 2 tests together to avoid racing conditions on the creation/deletion of directories
  it should "remove empty and non-empty directories" in {
    val stupidDirName = "lalalaXX-aDir"
    // empty directory test:
    withClue(s"Impossible to create empty directory '${stupidDirName}'") { assert(HDFS.getFileSystem.mkdirs(new Path(stupidDirName))) }
    withClue(s"Impossible to remove empty directory '${stupidDirName}'") { assert(HDFS.rm(stupidDirName)) }
    withClue(s"Empty directory '${stupidDirName}' exists after 'rm'!!!!") { assert(!HDFS.fileExists(stupidDirName)) }
    // non-empty directory test:
    withClue(s"Impossible to create non-empty directory '${stupidDirName}'") { assert(HDFS.getFileSystem.mkdirs(new Path(stupidDirName))) }
    val srcFileName = s"${stupidDirName}/luis.txt"
    withClue(s"Impossible to create HDFS file ${srcFileName}") { assert(writeASampleFile(srcFileName)) }
    withClue(s"HDFS file '${srcFileName}' does not exist after creation ") { assert(HDFS.fileExists(srcFileName)) }
    withClue(s"Impossible to remove non-empty directory '${stupidDirName}'") { assert(HDFS.rm(stupidDirName)) }
    withClue(s"Empty directory '${stupidDirName}' exists after 'rm'!!!!") { assert(!HDFS.fileExists(stupidDirName)) }
  }

}
