name := "fp-to-the-max"

version := "0.1"

scalaVersion := "2.13.0-M4"

scalacOptions in Compile += "-deprecation"

mainClass in (Compile, run) := Some("tc.meetup.Main")

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1212"
)
