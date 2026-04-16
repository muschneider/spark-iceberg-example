package com.example.spark

import java.nio.file.Paths
import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

object Main {
  private val WarehouseProperty = "calc.vp.warehouse"

  def newSparkSession(appName: String, extraConfigs: Map[String, String] = Map.empty): SparkSession = {
    val builder = extraConfigs.foldLeft(
      SparkSession
      .builder()
      .appName(appName)
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.testing.memory", "2147480000")
    ) { case (sessionBuilder, (key, value)) =>
      sessionBuilder.config(key, value)
    }

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    builder.getOrCreate()
  }

  def persistToIceberg(dataFrame: DataFrame, catalog: String = "local", tableName: String = "production.features"): Unit = {
    val businessKey = Seq("chave", "movimento")

    require(dataFrame.schema.fieldNames.contains("chave"), "Incoming DataFrame must contain chave")
    require(dataFrame.schema.fieldNames.contains("movimento"), "Incoming DataFrame must contain movimento")

    val spark = dataFrame.sparkSession
    val (namespaceParts, rawTableName) = splitTableIdentifier(tableName)
    val qualifiedTable = qualifyIdentifier(catalog, namespaceParts :+ rawTableName)
    val sourceColumns = dataFrame.schema.fieldNames.toSet

    if (namespaceParts.nonEmpty) {
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ${qualifyIdentifier(catalog, namespaceParts)}")
    }

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $qualifiedTable (
         |  ${schemaDefinition(dataFrame)}
         |)
         |USING iceberg
         |PARTITIONED BY (${quoteIdentifier("movimento")})
         |TBLPROPERTIES (
         |  'format-version' = '2',
         |  'write.merge.mode' = 'merge-on-read',
         |  'write.update.mode' = 'merge-on-read',
         |  'write.delete.mode' = 'merge-on-read'
         |)
         |""".stripMargin
    )

    spark.sql(
      s"""
         |ALTER TABLE $qualifiedTable SET TBLPROPERTIES (
         |  'format-version' = '2',
         |  'write.merge.mode' = 'merge-on-read',
         |  'write.update.mode' = 'merge-on-read',
         |  'write.delete.mode' = 'merge-on-read'
         |)
         |""".stripMargin
    )

    val targetSchemaBeforeEvolution = spark.table(qualifiedTable).schema
    val missingColumns = dataFrame.schema.fields.filterNot(field => targetSchemaBeforeEvolution.fieldNames.contains(field.name))

    if (missingColumns.nonEmpty) {
      val addColumnsSql = missingColumns
        .map(field => s"${quoteIdentifier(field.name)} ${field.dataType.catalogString}")
        .mkString(", ")

      spark.sql(s"ALTER TABLE $qualifiedTable ADD COLUMNS ($addColumnsSql)")
    }

    val targetSchema = spark.table(qualifiedTable).schema
    val carriedColumns = targetSchema.fieldNames.filterNot(sourceColumns.contains).filterNot(businessKey.contains)
    val sourceAlias = "source_batch"
    val existingAlias = "existing_rows"

    val alignedSource = if (carriedColumns.nonEmpty) {
      val existingValues = spark
        .table(qualifiedTable)
        .select((businessKey ++ carriedColumns).map(name => col(name)): _*)
        .alias(existingAlias)

      dataFrame
        .alias(sourceAlias)
        .join(existingValues, businessKey, "left")
        .select(targetSchema.fields.map { field =>
          if (sourceColumns.contains(field.name)) {
            col(s"$sourceAlias.${field.name}").cast(field.dataType).as(field.name)
          } else {
            col(s"$existingAlias.${field.name}").cast(field.dataType).as(field.name)
          }
        }: _*)
    } else {
      dataFrame.select(targetSchema.fields.map { field =>
        if (sourceColumns.contains(field.name)) {
          col(field.name).cast(field.dataType).as(field.name)
        } else {
          lit(null).cast(field.dataType).as(field.name)
        }
      }: _*)
    }

    val sourceView = s"iceberg_merge_source_${UUID.randomUUID().toString.replace('-', '_')}"
    alignedSource.createOrReplaceTempView(sourceView)

    try {
      spark.sql(
        s"""
           |MERGE INTO $qualifiedTable AS target
           |USING $sourceView AS source
           |ON target.${quoteIdentifier("chave")} = source.${quoteIdentifier("chave")}
           |AND target.${quoteIdentifier("movimento")} = source.${quoteIdentifier("movimento")}
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin
      )
    } finally {
      spark.catalog.dropTempView(sourceView)
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Parquet path must be provided as the first CLI argument")

    val parquetPath = args(0)
    val spark = newSparkSession(
      "calc-vp-staging",
      Map(
        "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type" -> "hadoop",
        "spark.sql.catalog.local.warehouse" -> defaultWarehousePath()
      )
    )

    try {
      persistToIceberg(spark.read.parquet(parquetPath))
      println(s"Persisted Parquet data from $parquetPath to local.production.features")
    } finally {
      spark.stop()
    }
  }

  private def defaultWarehousePath(): String = {
    val configuredPath = sys.props.get(WarehouseProperty).getOrElse("warehouse")
    Paths.get(configuredPath).toAbsolutePath.normalize.toUri.toString
  }

  private def splitTableIdentifier(tableName: String): (Seq[String], String) = {
    val parts = tableName.split("\\.").filter(_.nonEmpty).toSeq
    require(parts.nonEmpty, "Table name must not be empty")
    (parts.dropRight(1), parts.last)
  }

  private def schemaDefinition(dataFrame: DataFrame): String = {
    dataFrame.schema.fields
      .map(field => s"${quoteIdentifier(field.name)} ${field.dataType.catalogString}")
      .mkString(", ")
  }

  private def qualifyIdentifier(catalog: String, parts: Seq[String]): String = {
    (Seq(catalog) ++ parts).map(quoteIdentifier).mkString(".")
  }

  private def quoteIdentifier(identifier: String): String = {
    s"`${identifier.replace("`", "``")}`"
  }
}
