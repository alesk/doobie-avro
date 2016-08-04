name := "doobie-avro"

version := "1.0"

scalaVersion := "2.11.7"

lazy val doobieVersion = "0.3.0"

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-contrib-postgresql" % doobieVersion,
  "org.tpolecat" %% "doobie-contrib-specs2" % doobieVersion,
  "com.github.scopt" %% "scopt" % "3.5.0"
)
