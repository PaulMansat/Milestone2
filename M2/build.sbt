name := "Milestone2"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>

  "Milestone2_Group" + "04" + "." + artifact.extension
  
}
