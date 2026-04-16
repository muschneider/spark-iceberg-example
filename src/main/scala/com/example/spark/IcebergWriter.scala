package com.example.spark

import java.util.UUID

import com.example.spark.TableIdentifier.quote
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

final private[spark] class IcebergWriter(target: TableIdentifier) {

    import IcebergWriter._

    def upsert(batch: DataFrame): Unit = {
        val spark = batch.sparkSession
        validateBatchSchema(batch.schema)

        createNamespaceIfMissing(spark)
        createTableIfMissing(spark, batch.schema)
        applyTableProperties(spark)
        evolveSchema(spark, batch.schema)

        val targetSchema   = spark.table(target.qualifiedName).schema
        val batchColumnSet = batch.schema.fieldNames.toSet
        val targetOnlyCols = targetSchema.fieldNames.filterNot(batchColumnSet.contains)
        val updatableCols  = batch.schema.fieldNames.toSeq.diff(IcebergTableConfig.BusinessKey)

        val preparedSource = prepareSource(batch, targetSchema, batchColumnSet)

        val sourceView = s"iceberg_merge_source_${UUID.randomUUID().toString.replace('-', '_')}"
        preparedSource.createOrReplaceTempView(sourceView)
        try spark.sql(mergeSql(sourceView, updatableCols, targetSchema, targetOnlyCols, batchColumnSet))
        finally spark.catalog.dropTempView(sourceView)
    }

    private def validateBatchSchema(schema: StructType): Unit = {
        val missing = IcebergTableConfig.BusinessKey.filterNot(schema.fieldNames.contains)
        require(missing.isEmpty, s"Incoming DataFrame must contain ${missing.mkString(", ")}")
    }

    private def createNamespaceIfMissing(spark: SparkSession): Unit = target.qualifiedNamespace.foreach(ns =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $ns")
    )

    private def createTableIfMissing(spark: SparkSession, batchSchema: StructType): Unit = {
        val columns = batchSchema.fields
            .map(f => s"${quote(f.name)} ${f.dataType.catalogString}")
            .mkString(",\n  ")

        spark.sql(
            s"""CREATE TABLE IF NOT EXISTS ${target.qualifiedName} (
         |  $columns
         |)
         |USING iceberg
         |PARTITIONED BY (${quote(IcebergTableConfig.PartitionColumn)})
         |TBLPROPERTIES (
         |  ${IcebergTableConfig.renderPropertiesSql}
         |)""".stripMargin
        )
    }

    /** Keeps properties on pre-existing tables aligned with the current configuration. */
    private def applyTableProperties(spark: SparkSession): Unit = spark.sql(
        s"""ALTER TABLE ${target.qualifiedName} SET TBLPROPERTIES (
         |  ${IcebergTableConfig.renderPropertiesSql}
         |)""".stripMargin
    )

    private def evolveSchema(spark: SparkSession, batchSchema: StructType): Unit = {
        val existingFieldNames = spark.table(target.qualifiedName).schema.fieldNames.toSet
        val newFields          = batchSchema.fields.filterNot(f => existingFieldNames.contains(f.name))
        if (newFields.nonEmpty) {
            val additions = newFields
                .map(f => s"${quote(f.name)} ${f.dataType.catalogString}")
                .mkString(", ")
            spark.sql(s"ALTER TABLE ${target.qualifiedName} ADD COLUMNS ($additions)")
        }
    }

    private def prepareSource(batch: DataFrame, targetSchema: StructType, batchColumns: Set[String]): DataFrame = {
        import org.apache.spark.sql.functions.col

        val typedByTarget: Map[String, DataType] = targetSchema.fields.iterator.map(f => f.name -> f.dataType).toMap

        val projected = batch.schema.fieldNames.map { name =>
            typedByTarget.get(name) match {
                case Some(dt) => col(name).cast(dt).as(name)
                case None     => col(name) // will be handled via schema evolution before MERGE
            }
        }

        val projectedDf = batch.select(projected: _*)

        if (batchColumns.contains(IcebergTableConfig.PartitionColumn))
            projectedDf.repartition(col(IcebergTableConfig.PartitionColumn))
        else
            projectedDf
    }

    private def mergeSql(sourceView: String,
                         updatableCols: Seq[String],
                         targetSchema: StructType,
                         targetOnlyCols: Seq[String],
                         batchColumns: Set[String]
    ): String = {
        val onClause = IcebergTableConfig.BusinessKey
            .map(k => s"target.${quote(k)} = source.${quote(k)}")
            .mkString(" AND ")

        val updateSet =
            if (updatableCols.isEmpty)
                ""
            else {
                val assignments = updatableCols
                    .map(c => s"  target.${quote(c)} = source.${quote(c)}")
                    .mkString(",\n")
                s"\nWHEN MATCHED THEN UPDATE SET\n$assignments"
            }

        val targetByName: Map[String, StructField] = targetSchema.fields.iterator.map(f => f.name -> f).toMap

        val insertColumns = targetSchema.fieldNames.map(quote).mkString(", ")
        val insertValues = targetSchema.fieldNames
            .map { name =>
                if (batchColumns.contains(name))
                    s"source.${quote(name)}"
                else
                    s"CAST(NULL AS ${targetByName(name).dataType.catalogString})"
            }
            .mkString(", ")

        s"""MERGE INTO ${target.qualifiedName} AS target
       |USING $sourceView AS source
       |ON $onClause$updateSet
       |WHEN NOT MATCHED THEN INSERT ($insertColumns) VALUES ($insertValues)""".stripMargin
    }

}

private[spark] object IcebergWriter {

    def upsert(batch: DataFrame, catalog: String, dottedTableName: String): Unit =
        new IcebergWriter(TableIdentifier.parse(catalog, dottedTableName)).upsert(batch)

}
