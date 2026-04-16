package com.example.spark

/**
 * Iceberg table layout and write properties for this project.
 *
 * The workload is update-heavy (>99% of rows are updates), so we lean on merge-on-read
 * to avoid full-file rewrites and partition writes by `movimento` so MERGE scans
 * prune aggressively. `write.distribution-mode=hash` ensures source rows are
 * shuffled per partition key before the write, which matters on cluster runs
 * (e.g. Dataproc) even though it's a no-op for the local `local[1]` tests.
 */
private[spark] object IcebergTableConfig {

  /** Business key for upserts. Order matters only for display/logging. */
  val BusinessKey: Seq[String] = Seq("chave", "movimento")

  val PartitionColumn: String = "movimento"

  /** Properties applied at both CREATE and ALTER time so existing tables converge. */
  val TableProperties: Seq[(String, String)] = Seq(
    "format-version"         -> "2",
    "write.merge.mode"       -> "merge-on-read",
    "write.update.mode"      -> "merge-on-read",
    "write.delete.mode"      -> "merge-on-read",
    "write.distribution-mode" -> "hash"
  )

  /** Rendered `key = 'value', ...` fragment for DDL statements. */
  def renderPropertiesSql: String =
    TableProperties.map { case (k, v) => s"'$k' = '$v'" }.mkString(",\n  ")
}
