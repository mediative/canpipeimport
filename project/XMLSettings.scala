import sbt.Keys._
import sbt._

object XMLSettings {

  private val logResolvers = Seq(
    "Scales Repo" at "http://scala-scales.googlecode.com/svn/repo"
  )

  private val deps = Seq(
    // just for the core library
    "org.scalesxml" % "scales-xml_2.10" % "0.6.0-M3"
    // and additionally use these for String based XPaths
    ,"org.scalesxml" %% "scales-jaxen" % "0.5.0" intransitive(),
    "jaxen" % "jaxen" % "1.1.3" intransitive(),
    // to use Aalto based parsing
    "org.scalesxml" %% "scales-aalto" % "0.5.0"
  )

  val xmlSettings = Seq(
    resolvers ++= logResolvers,
    libraryDependencies ++= deps
  )

}
