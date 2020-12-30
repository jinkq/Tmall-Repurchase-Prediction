name := "Repurchase Prediction"
version := "1.0"
scalaVersion := "2.11.12"
mainClass := Some("GetFeatures")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib-local" % "2.3.0"