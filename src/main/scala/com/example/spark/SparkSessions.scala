package com.example.spark

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

/** SparkSession factories for local tests/runs and for cluster runs (e.g. Dataproc).
  *
  * When running under `spark-submit` on a cluster, the session is built with only the Iceberg SQL extensions and no
  * master/driver overrides, so Dataproc's YARN configuration wins. Locally, a deterministic `local[1]` session is used.
  */
private[spark] object SparkSessions {

    val WarehousePathProperty: String = "calc.feat.warehouse"
    val DefaultWarehouseDir: String   = "warehouse"

    /** Build a local SparkSession suitable for tests and single-node runs.
      *
      * Callers can pass `extraConfigs` to register Iceberg extensions/catalogs on top of the local defaults (ui
      * disabled, driver bound to 127.0.0.1, 1 shuffle partition).
      */
    def local(appName: String, extraConfigs: Map[String, String] = Map.empty): SparkSession = {
        val base = SparkSession
            .builder()
            .appName(appName)
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.testing.memory", "2147480000")

        val builder = extraConfigs.foldLeft(base) { case (b, (k, v)) => b.config(k, v) }

        // Clear any prior active/default session to keep repeated test runs isolated.
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()

        builder.getOrCreate()
    }

    /** Build a SparkSession that respects the ambient cluster configuration (e.g. from `spark-submit` on Dataproc).
      * Only Iceberg SQL extensions are registered here; catalog, master, memory, and tuning are expected to come from
      * the launcher.
      */
    def cluster(appName: String): SparkSession = SparkSession
        .builder()
        .appName(appName)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .getOrCreate()

    /** Build a session for a local run, wiring the `local` Iceberg Hadoop catalog to either the configured warehouse
      * path or the project-relative default.
      */
    def localWithLocalIcebergCatalog(appName: String): SparkSession = local(
        appName,
        Map(
            "spark.sql.extensions"              -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.local"           -> "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type"      -> "hadoop",
            "spark.sql.catalog.local.warehouse" -> defaultWarehouseUri()
        )
    )

    /** Resolve the local warehouse directory as an absolute `file:` URI. */
    def defaultWarehouseUri(): String = {
        val configured = sys.props.getOrElse(WarehousePathProperty, DefaultWarehouseDir)
        Paths.get(configured).toAbsolutePath.normalize.toUri.toString
    }

    /** Heuristic: treat the run as a cluster run when `spark-submit` sets either `SPARK_SUBMIT` in the environment or
      * `spark.submit.deployMode` as a system property. Falls back to local.
      */
    def isClusterRun: Boolean =
        sys.env.contains("SPARK_SUBMIT") ||
            sys.props.contains("spark.submit.deployMode") ||
            sys.props.get("spark.master").exists(m => !m.startsWith("local"))

}
