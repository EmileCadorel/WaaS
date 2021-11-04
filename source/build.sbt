
name := "orch"

version := "1.0"

scalaVersion := "2.11.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.26"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.26"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.26"
// libraryDependencies += "org.jline" % "jline" % "3.21.0"
libraryDependencies += "com.github.scopt" % "scopt_2.11" % "4.0.0-RC2"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"
libraryDependencies += "org.yaml" % "snakeyaml" % "1.8"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7"
libraryDependencies += "com.jcabi" % "jcabi-ssh" % "1.6.1"
libraryDependencies += "com.jcraft" % "jsch" % "0.1.55"
libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.11"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.5"
