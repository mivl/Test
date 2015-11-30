name := "Test"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1" % "provided"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.11.2"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.0"

//libraryDependencies += "org.alitouka" % "spark_dbscan_2.10" % "0.0.4"
//resolvers += "Aliaksei Litouka's repository" at "http://alitouka-public.s3-website-us-east-1.amazonaws.com/"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.thesamet" %% "kdtree" % "1.0.4"