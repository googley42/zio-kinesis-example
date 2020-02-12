name := "zio-kinesis-example"

version := "0.1"

scalaVersion := "2.12.8"

val circe = "0.12.0-M1"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
  "dev.zio" %% "zio-streams" % "1.0.0-RC17",
  "dev.zio" %% "zio-test" % "1.0.0-RC17" % "test",
  "dev.zio" %% "zio-test-sbt" % "1.0.0-RC17" % "test",
  "nl.vroste" %% "zio-kinesis" % "0.3.0",
  "io.circe" %% "circe-core" % circe,
  "io.circe" %% "circe-parser" % circe,
  "io.circe" %% "circe-generic" % circe,
  "io.circe" %% "circe-literal" % circe
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
