name := "wikilink"

organization := "edu.umass.cs.iesl.wikilink"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.1"

resolvers ++= Seq(
    "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository",
    "conjars.org" at "http://conjars.org/repo"
)

scalacOptions ++= Seq("-unchecked","-deprecation")

libraryDependencies ++= Seq(
     "org.scalatest" %% "scalatest" % "1.6.1" % "test",
     "junit" % "junit" % "4.8.1",
     "net.databinder" %% "dispatch-http" % "0.8.8",
     "cc.refectorie.user.sameer" % "util" % "1.1-SNAPSHOT",
     "commons-net" % "commons-net" % "2.0", // this is for FTP support and is currently not used.
     "commons-io" % "commons-io" % "2.0", // this is for one line and could probably be removed
     "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2"
)

fork in run := true

fork in runMain := true

javaOptions in run += "-Xmx20G"

javaOptions in runMain += "-Xmx20G"
