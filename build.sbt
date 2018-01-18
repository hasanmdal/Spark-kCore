name := "KCore"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "HDP Releases" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Hadoop-jetty" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.1" 

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0.2.2.0.23-4" excludeAll(
  ExclusionRule(organization = "org.mortbay.jetty", name = "jetty"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "jetty-util"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "jsp-api-2.1"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "jsp-2.1"),
  ExclusionRule(organization = "tomcat", name = "jasper-runtime"),
  ExclusionRule(organization = "tomcat", name = "jasper-compiler")
  )


libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.1.1"