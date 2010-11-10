import sbt._

class CasbahMapperProject(info: ProjectInfo) extends DefaultProject(info) {
  override def managedStyle = ManagedStyle.Maven
  super.compileOptions ++ Seq(Unchecked, ExplainTypes, Deprecation)

  val casbah_core = "com.mongodb.casbah" %% "core" % "2.0b2"
  val objenesis = "org.objenesis" % "objenesis" % "1.2"

  val specsVersion = crossScalaVersionString match {
    case "2.8.0" => "1.6.5"
    case "2.8.1" => "1.6.6"
  }
  val specs = "org.scala-tools.testing" %% "specs" % specsVersion % "test->default"
  val commonsLang = "commons-lang" % "commons-lang" % "2.5" % "test->default"

  val publishTo = Resolver.sftp("repobum", "repobum", "/home/public/%s".format(
    if (projectVersion.value.toString.endsWith("-SNAPSHOT")) "snapshots"
    else "releases"
  )) as("repobum_repobum", new java.io.File(Path.userHome + "/.ssh/repobum"))

  val scalaToolsRepo = "Scala Tools Release Repository" at "http://scala-tools.org/repo-releases"
  val scalaToolsSnapRepo = "Scala Tools Snapshot Repository" at "http://scala-tools.org/repo-snapshots"
  val bumRepo = "Bum Networks Release Repository" at "http://repo.bumnetworks.com/releases/"
  val bumSnapsRepo = "Bum Networks Snapshots Repository" at "http://repo.bumnetworks.com/snapshots/"
}
