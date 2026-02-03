name := "PipelineVendasGold"

version := "0.1"

scalaVersion := "2.12.18" // Versão padrão compatível com Spark 3.x

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.crealytics"   %% "spark-excel" % "3.5.0_0.20.3"
)