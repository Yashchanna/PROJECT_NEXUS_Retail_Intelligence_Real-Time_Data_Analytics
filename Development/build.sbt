ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.2.0"
ThisBuild / organization := "com.nexus"
name := "nexus-realtime"

val sparkVersion = "3.5.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.10.1",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

Test / fork := true
Test / parallelExecution := false
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "-Xms512m", "-Xmx3g",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
Test / javaOptions ++= (Compile / run / javaOptions).value
Compile / run / javaOptions += "-Djava.library.path=C:\\hadoop\\bin"

