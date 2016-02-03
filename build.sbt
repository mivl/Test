name := "Test"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1" % "provided"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.11.2"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.thesamet" %% "kdtree" % "1.0.4"