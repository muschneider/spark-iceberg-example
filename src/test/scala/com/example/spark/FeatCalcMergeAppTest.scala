package com.example.spark

import java.nio.file.{Files, Path}

import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class FeatCalcMergeAppTest {

    @Test
    def createsWorkingSparkSession(): Unit = {
        val spark = newIcebergSparkSession()

        try assertEquals(3L, spark.range(3).count())
        finally spark.stop()
    }

    @Test
    def createsPartitionedIcebergTableWithUpdateFriendlyProperties(): Unit = {
        val spark = newIcebergSparkSession()
        import spark.implicits._

        try {
            val input = Seq(
                ("a", "2024-01-01", 10, 20)
            ).toDF("chave", "movimento", "vp1", "vp2")

            FeatCalcMergeApp.persistToIceberg(input)

            val createStatement = spark
                .sql("SHOW CREATE TABLE local.production.features")
                .collect()
                .map(_.getString(0))
                .mkString("\n")
            val properties =
                spark
                    .sql("SHOW TBLPROPERTIES local.production.features")
                    .collect()
                    .map(row => row.getString(0) -> row.getString(1))
                    .toMap

            assertTrue(createStatement.contains("PARTITIONED BY"))
            assertTrue(createStatement.contains("movimento"))
            assertEquals("2", properties("format-version"))
            assertEquals("merge-on-read", properties("write.merge.mode"))
            assertEquals("merge-on-read", properties("write.update.mode"))
            assertEquals("merge-on-read", properties("write.delete.mode"))
        } finally spark.stop()
    }

    @Test
    def mergesRowsAndEvolvesSchemaWithoutNullingMissingColumns(): Unit = {
        val spark = newIcebergSparkSession()
        import spark.implicits._

        try {
            val initialBatch = Seq(
                ("a", "2024-01-01", 10, 20),
                ("b", "2024-01-01", 30, 40)
            ).toDF("chave", "movimento", "vp1", "vp2")

            FeatCalcMergeApp.persistToIceberg(initialBatch)

            val secondBatch = Seq(
                ("a", "2024-01-01", 99),
                ("c", "2024-01-01", 55)
            ).toDF("chave", "movimento", "vp4")

            FeatCalcMergeApp.persistToIceberg(secondBatch)

            val rowsByKey =
                spark
                    .table("local.production.features")
                    .orderBy("chave")
                    .collect()
                    .map(row => row.getAs[String]("chave") -> row)
                    .toMap

            assertEquals(3, rowsByKey.size)
            assertTrue(spark.table("local.production.features").schema.fieldNames.contains("vp4"))

            val updatedRow = rowsByKey("a")
            assertEquals(10, updatedRow.getAs[Int]("vp1"))
            assertEquals(20, updatedRow.getAs[Int]("vp2"))
            assertEquals(99, updatedRow.getAs[Int]("vp4"))

            val unchangedRow = rowsByKey("b")
            assertEquals(30, unchangedRow.getAs[Int]("vp1"))
            assertEquals(40, unchangedRow.getAs[Int]("vp2"))
            assertNull(unchangedRow.getAs[Any]("vp4"))

            val insertedRow = rowsByKey("c")
            assertNull(insertedRow.getAs[Any]("vp1"))
            assertNull(insertedRow.getAs[Any]("vp2"))
            assertEquals(55, insertedRow.getAs[Int]("vp4"))
        } finally spark.stop()
    }

    @Test
    def mainLoadsParquetAndPersistsToIceberg(): Unit = {
        val parquetPath       = Files.createTempDirectory("main-parquet-input")
        val warehouse         = Files.createTempDirectory("main-iceberg-warehouse")
        val propertyName      = "calc.vp.warehouse"
        val previousWarehouse = Option(System.getProperty(propertyName))
        val writerSpark       = newIcebergSparkSession()
        import writerSpark.implicits._

        try Seq(
                ("a", "2024-01-01", 10, 20),
                ("b", "2024-01-01", 30, 40)
            ).toDF("chave", "movimento", "vp1", "vp2")
                .write
                .mode("overwrite")
                .parquet(parquetPath.toString)
        finally writerSpark.stop()

        try {
            System.setProperty(propertyName, warehouse.toString)
            val verificationSpark = newIcebergSparkSession(warehouse)

            try {
                val rows = verificationSpark
                    .table("local.production.features")
                    .orderBy("chave")
                    .collect()

                assertEquals(2, rows.length)
                assertEquals("a", rows(0).getAs[String]("chave"))
                assertEquals(10, rows(0).getAs[Int]("vp1"))
                assertEquals("b", rows(1).getAs[String]("chave"))
                assertEquals(40, rows(1).getAs[Int]("vp2"))
            } finally verificationSpark.stop()
        } finally
            previousWarehouse match {
                case Some(value) => System.setProperty(propertyName, value)
                case None        => System.clearProperty(propertyName)
            }
    }

    private def newIcebergSparkSession(warehouse: Path = Files.createTempDirectory("iceberg-warehouse")) =
        org.apache.spark.sql.SparkSession
            .builder()
            .appName("iceberg-test-session")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.testing.memory", "2147480000")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toString)
            .getOrCreate()

}
