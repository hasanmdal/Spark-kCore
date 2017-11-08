name := "KCore"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "HDP Releases" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0.2.2.0.23-4"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.1.1"

excludeDependencies ++= Seq(

  SbtExclusionRule("org.mortbay.jetty", "jetty"),
  SbtExclusionRule("org.mortbay.jetty", "jetty-util"),
  SbtExclusionRule("org.mortbay.jetty", "jsp-api-2.1"),
  SbtExclusionRule("org.mortbay.jetty", "jsp-2.1"),
  SbtExclusionRule("tomcat", "jasper-runtime"),
  SbtExclusionRule("tomcat", "jasper-compiler")
)