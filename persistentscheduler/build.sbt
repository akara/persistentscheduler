import com.lightbend.sbt.SbtAspectj.aspectjSettings
import Versions._

packSettings

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
  "org.squbs" %% "squbs-ext" % squbsV,
  "com.ebay.squbs" %% "rocksqubs-mayfly" % squbsV,
  "org.scalatest" %% "scalatest" % scalaTestV % "test",
  "org.squbs" %% "squbs-testkit" % squbsV % "test"
)

PB.targets in Compile := Seq(
  scalapb.gen(grpc = false) -> (sourceManaged in Compile).value
)

aspectjSettings ++ Seq(
  aspectjVersion := aspectjV,
  javaOptions in reStart ++= (aspectjWeaverOptions in Aspectj).value,
  javaOptions in run ++= (aspectjWeaverOptions in Aspectj).value
)

mainClass in (Compile, run) := Some("org.squbs.unicomplex.Bootstrap")
