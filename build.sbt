ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ProducerConsumer"
  )

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % "2.7.0",
    "com.typesafe.akka" %% "akka-http" % "10.4.0",
    "com.typesafe.akka" %% "akka-actor-typed" % "2.7.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "ch.qos.logback" % "logback-classic" % "1.4.5",
    "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "5.0.0",
    "org.json4s" %% "json4s-native" % "4.0.6",
    "org.json4s" %% "json4s-jackson" % "4.0.6",
    "com.typesafe.akka" %% "akka-http-core" % "10.4.0"
)