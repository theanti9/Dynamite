name := "Dynamite"

version := "1.0"

resolvers ++= Seq(
  "repository.jboss.org" at "https://repository.jboss.org/nexus/content/repositories/releases/",
  "Scala Tools" at "https://oss.sonatype.org/content/repositories/snapshots"
)
    
libraryDependencies ++= Seq(
	"io.netty" % "netty-all" % "4.0.19.Final-SNAPSHOT",
)