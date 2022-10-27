name := "ProjectSpark"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0",
                             "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
                             "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
                             "org.apache.spark" % "spark-hive_2.11" % "2.1.0",
                             "org.postgresql" % "postgresql" % "42.2.14"
                              )

