name := "LinearRegression"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion="2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.5.0"
)




resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)
resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"