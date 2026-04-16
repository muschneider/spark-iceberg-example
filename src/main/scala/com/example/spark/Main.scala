package com.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** CLI entrypoint.
  *
  * Loads a Parquet dataset and upserts it into the configured Iceberg table. The same code path runs locally (tests,
  * `java -jar`) and on a managed cluster like Google Cloud Dataproc; cluster vs. local is auto-detected by
  * [[SparkSessions.isClusterRun]] and can be forced via system properties.
  *
  * Test surface (do not rename without updating `MainTest`):
  *   - [[Main.newSparkSession]]
  *   - [[Main.persistToIceberg]]
  *   - [[Main.main]]
  */
object Main {

    private val AppName          = "spark-iceberg-example"
    private val DefaultCatalog   = "local"
    private val DefaultTableName = "production.features"

    def newSparkSession(appName: String, extraConfigs: Map[String, String] = Map.empty): SparkSession = SparkSessions
        .local(appName, extraConfigs)

    /** Upserts `dataFrame` into the given Iceberg table using the (chave, movimento) business key. */
    def persistToIceberg(
        dataFrame: DataFrame,
        catalog: String = DefaultCatalog,
        tableName: String = DefaultTableName
    ): Unit = IcebergWriter.upsert(dataFrame, catalog, tableName)

    def main(args: Array[String]): Unit = {
        require(args.nonEmpty, "Parquet path must be provided as the first CLI argument")
        val parquetPath = args(0)

        val spark =
            if (SparkSessions.isClusterRun)
                SparkSessions.cluster(AppName)
            else
                SparkSessions.localWithLocalIcebergCatalog(AppName)

        try {
            persistToIceberg(spark.read.parquet(parquetPath))
            println(s"Persisted Parquet data from $parquetPath to $DefaultCatalog.$DefaultTableName")
        } finally spark.stop()
    }

}
