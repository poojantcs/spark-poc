package com.example

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkOrderByExample {
  private val localInputFilePathPrefix = "src/main/resources/sap_bom/"
  private val localOutputFilePathPrefix = "target/test-output/"
  private val gcsInputFilePathPrefix = "gs://spark_scala_testing/input/"
  private val gcsOutputFilePathPrefix = "gs://spark_scala_testing/output/"
  private var inputFilePathPrefix = ""
  private var outputFilePathPrefix = ""

  def main(args: Array[String]): Unit = {
    // Create a Spark Session - Local
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SampleSparkProgramInScala")
      .master("local[*]")
      .getOrCreate()

    runDataframeOrderBy(args, spark)

    spark.stop()
  }

  private def initializeFilePath(executionType: String, datasetSize: String, outputPrefix: String): Unit = {

    if (executionType == "local") {
      inputFilePathPrefix = localInputFilePathPrefix
      outputFilePathPrefix = localOutputFilePathPrefix
    } else if (executionType == "GCS") {
      inputFilePathPrefix = gcsInputFilePathPrefix + datasetSize + "/"
      outputFilePathPrefix = gcsOutputFilePathPrefix + datasetSize + "/" + outputPrefix + "/"
    }

    println("Input file path prefix: " + inputFilePathPrefix)
    println("Output file path prefix: " + outputFilePathPrefix)
  }

  private def writeOutput(df1: DataFrame, partitionNumber: Int, outputFilePath: String): Unit = {
    df1.show()
    df1
      .coalesce(partitionNumber)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputFilePath)
  }

  private def doDfJoinAndWriteOutput(df1: DataFrame, df2: DataFrame, joinExpr: Column, joinType: String, partitionNumber: Int, outputFilePath: String): DataFrame = {
    val joinedDf: DataFrame = df1.join(df2, joinExpr, joinType)
    writeOutput(joinedDf, partitionNumber, outputFilePath)

    joinedDf
  }

  private def runDataframeOrderBy(args: Array[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val executionType = args(0)
    val datasetSize = args(1)
    val outputPrefix = args(2)

    initializeFilePath(executionType, datasetSize, outputPrefix)

    // Input file paths
    var mastInputPath = inputFilePathPrefix + "MAST.tab"
    var stasInputPath = inputFilePathPrefix + "STAS.tab"
    var stkoInputPath = inputFilePathPrefix + "STKO.tab"
    var marcInputPath = inputFilePathPrefix + "MARC.tab"

    // Output file paths
    var mast_ordered_output_path = outputFilePathPrefix + "MAST_ordered"
    var mast_stas_inner_output_path = outputFilePathPrefix + "MAST_STAS_inner"
    var mast_stas_stko_inner_output_path = outputFilePathPrefix + "MAST_STAS_STKO_inner"
    var mast_stas_sorted_output_path = outputFilePathPrefix + "MAST_STAS_sorted"
    var mast_stas_leftouter_output_path = outputFilePathPrefix + "MAST_STAS_leftOuter"
    var mast_stas_stko_leftouter_output_path = outputFilePathPrefix + "MAST_STAS_STKO_leftOuter"
    var mast_stas_inner_join_rejects_output_path = outputFilePathPrefix + "MAST_STAS_innerJoinRejects"
    var mast_stas_stko_inner_join_rejects_output_path = outputFilePathPrefix + "MAST_STAS_STKO_innerJoinRejects"
    var mast_stas_marc_inner_output_path = outputFilePathPrefix + "MAST_MARC_inner"
    var effective_in_output_path = outputFilePathPrefix + "Effective_In"
    var effective_out_output_path = outputFilePathPrefix + "Effective_Out"

    // Create DataFrames based on the content of a CSV/Tab file
    // MAST
    println("MAST file path" + mastInputPath)
    val mastDF_orig = spark.read.option("header", true).option("delimiter", "\t").csv(mastInputPath)
    val mastDF = mastDF_orig
      .withColumnRenamed("MANDT", "MAST_MANDT")
      .withColumnRenamed("MATNR", "MAST_MATNR")
      .withColumnRenamed("STLAL", "MAST_STLAL")
      .withColumnRenamed("STLAN", "MAST_STLAN")
      .withColumnRenamed("STLNR", "MAST_STLNR")
      .withColumnRenamed("WERKS", "MAST_WERKS")
      .withColumnRenamed("CSLTY", "MAST_CSLTY")
      .withColumnRenamed("LOSBS", "MAST_LOSBS")
      .withColumnRenamed("LOSVN", "MAST_LOSVN")

    // STAS
    val stasDF_orig = spark.read.option("header", true).option("delimiter", "\t").csv(stasInputPath)
    var stasDF = stasDF_orig
      .withColumnRenamed("MANDT", "STAS_MANDT")
      .withColumnRenamed("STASZ", "STAS_STASZ")
      .withColumnRenamed("STLAL", "STAS_STLAL")
      .withColumnRenamed("STLKN", "STAS_STLKN")
      .withColumnRenamed("STLNR", "STAS_STLNR")
      .withColumnRenamed("STLTY", "STAS_STLTY")
      .withColumnRenamed("AENNR", "STAS_AENNR")
      .withColumnRenamed("DATUV", "STAS_DATUV")
      .withColumnRenamed("LKENZ", "STAS_LKENZ")
      .withColumnRenamed("STVKN", "STAS_STVKN")

    // STKO
    val stkoDF_orig = spark.read.option("header", true).option("delimiter", "\t").csv(stkoInputPath)
    val stkoDF = stkoDF_orig
      .withColumnRenamed("MANDT", "STKO_MANDT")
      .withColumnRenamed("STKOZ", "STKO_STKOZ")
      .withColumnRenamed("STLAL", "STKO_STLAL")
      .withColumnRenamed("STLNR", "STKO_STLNR")
      .withColumnRenamed("STLTY", "STKO_STLTY")
      .withColumnRenamed("BMEIN", "STKO_BMEIN")
      .withColumnRenamed("BMENG", "STKO_BMENG")
      .withColumnRenamed("DATUV", "STKO_DATUV")
      .withColumnRenamed("LKENZ", "STKO_LKENZ")
      .withColumnRenamed("LOEKZ", "STKO_LOEKZ")
      .withColumnRenamed("STLST", "STKO_STLST")
      .withColumnRenamed("VGKZL", "STKO_VGKZL")

    // MARC
    val marcDF = spark.read.option("header", true).option("delimiter", "\t").csv(marcInputPath)

    // Join: MAST - STAS
    val MAST_STAS_inner = doDfJoinAndWriteOutput(
      mastDF,
      stasDF,
      mastDF("MAST_STLAL") === stasDF("STAS_STLAL") && mastDF("MAST_STLNR") === stasDF("STAS_STLNR"),
      "inner",
      1,
      mast_stas_inner_output_path)

    // OrderBy: MAST
    val MAST_orderBy = mastDF.orderBy($"MAST_STLAL".asc, $"MAST_STLAN".asc, $"MAST_STLNR".asc)
    writeOutput(MAST_orderBy, 1, mast_ordered_output_path)
  }

}
