package com.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object FeatCalcMergeApp {

    private val AppName          = "feat-calc-merge-app"
    private val DefaultCatalog   = "local"
    private val DefaultTableName = "production.features"

    def persistToIceberg(
        dataFrame: DataFrame,
        catalog: String = DefaultCatalog,
        tableName: String = DefaultTableName
    ): Unit = IcebergWriter.upsert(dataFrame, catalog, tableName)

    def newSparkSession(appName: String): SparkSession = SparkSession
        .builder()
        .appName(AppName)
        .getOrCreate()

    def main(args: Array[String]): Unit = {
        val spark = newSparkSession(AppName)

        val parquetPath = spark.sparkContext.getConf.get("spark.featcalcmergeapp.parquetPath")

        try {
            persistToIceberg(spark.read.parquet(parquetPath))
            println(s"Persisted Parquet data from $parquetPath to $DefaultCatalog.$DefaultTableName")
        } finally spark.stop()
    }

}
