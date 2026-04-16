package com.example.spark

private[spark] object IcebergTableConfig {

    val BusinessKey: Seq[String] = Seq("chave", "movimento")
    val PartitionColumn: String  = "movimento"

    val TableProperties: Seq[(String, String)] = Seq(
        "format-version"          -> "2",
        "write.merge.mode"        -> "merge-on-read",
        "write.update.mode"       -> "merge-on-read",
        "write.delete.mode"       -> "merge-on-read",
        "write.distribution-mode" -> "hash"
    )

    def renderPropertiesSql: String = TableProperties.map { case (k, v) => s"'$k' = '$v'" }.mkString(",\n  ")
}
