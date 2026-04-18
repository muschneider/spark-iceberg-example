package com.example.spark

private[spark] object IcebergTableConfig {

    val BusinessKey: Seq[String] = Seq("chave", "movimento")
    val PartitionColumn: String  = "movimento"

    val TableProperties: Seq[(String, String)] = Seq(
        "format-version"                             -> "2",
        "write.merge.mode"                           -> "merge-on-read",
        "write.update.mode"                          -> "merge-on-read",
        "write.delete.mode"                          -> "merge-on-read",
        "write.distribution-mode"                    -> "hash",
        // Keep MoR data/delete files sized so compaction stays cheap and reads stay vectorized.
        "write.target-file-size-bytes"               -> "268435456", // 256 MB
        "write.delete.target-file-size-bytes"        -> "67108864",  // 64  MB
        "write.parquet.compression-codec"            -> "zstd",
        "write.parquet.row-group-size-bytes"         -> "134217728", // 128 MB
        // Allow unsorted fan-out writes when hash distribution can't fully co-locate.
        "write.spark.fanout.enabled"                 -> "true",
        // Trim metadata.json history so planning stays fast on update-heavy tables.
        "write.metadata.delete-after-commit.enabled" -> "true",
        "write.metadata.previous-versions-max"       -> "10",
        // Vectorized reads + matching split size for downstream consumers.
        "read.parquet.vectorization.enabled"         -> "true",
        "read.split.target-size"                     -> "134217728",
        // Survive concurrent commits from compaction / rewrite jobs running out-of-band.
        "commit.retry.num-retries"                   -> "8"
    )

    def renderPropertiesSql: String = TableProperties.map { case (k, v) => s"'$k' = '$v'" }.mkString(",\n  ")
}
