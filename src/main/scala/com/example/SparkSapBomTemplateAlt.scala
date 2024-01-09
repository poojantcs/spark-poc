package com.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkSapBomTemplateAlt {
  var executionType, datasetSize, outputPrefix, inputFilePathPrefix, outputFilePathPrefix = ""
  private val localInputFilePathPrefix = "src/main/resources/sap_bom/"
  private val localOutputFilePathPrefix = "target/test-output/"
  private val gcsInputFilePathPrefix = "gs://spark_scala_testing/input/"
  private val gcsOutputFilePathPrefix = "gs://spark_scala_testing/output/"

  // Input file paths
  var mastInputPath, stasInputPath, stkoInputPath, marcInputPath = ""

  // Output file paths
  var mast_ordered_output_path, mast_stas_inner_output_path, mast_stas_stko_inner_output_path, mast_stas_sorted_output_path, mast_stas_leftouter_output_path,
  mast_stas_stko_leftouter_output_path, mast_stas_inner_join_rejects_output_path, mast_stas_stko_inner_join_rejects_output_path, mast_stas_marc_inner_output_path,
  effective_in_output_path, effective_out_output_path = ""


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SampleSparkProgramInScala")
      .getOrCreate()

    executionType = args(0)
    datasetSize = args(1)
    outputPrefix = args(2)

    initializeFilePath(executionType, datasetSize, outputPrefix)
    runSapBomTemplate(spark)

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

    // Input file paths
    mastInputPath = inputFilePathPrefix + "MAST.tab"
    stasInputPath = inputFilePathPrefix + "STAS.tab"
    stkoInputPath = inputFilePathPrefix + "STKO.tab"
    marcInputPath = inputFilePathPrefix + "MARC.tab"

    // Output file paths
    mast_ordered_output_path = outputFilePathPrefix + "MAST_ordered"
    mast_stas_inner_output_path = outputFilePathPrefix + "MAST_STAS_inner"
    mast_stas_stko_inner_output_path = outputFilePathPrefix + "MAST_STAS_STKO_inner"
    mast_stas_sorted_output_path = outputFilePathPrefix + "MAST_STAS_sorted"
    mast_stas_leftouter_output_path = outputFilePathPrefix + "MAST_STAS_leftOuter"
    mast_stas_stko_leftouter_output_path = outputFilePathPrefix + "MAST_STAS_STKO_leftOuter"
    mast_stas_inner_join_rejects_output_path = outputFilePathPrefix + "MAST_STAS_innerJoinRejects"
    mast_stas_stko_inner_join_rejects_output_path = outputFilePathPrefix + "MAST_STAS_STKO_innerJoinRejects"
    mast_stas_marc_inner_output_path = outputFilePathPrefix + "MAST_MARC_inner"
    effective_in_output_path = outputFilePathPrefix + "Effective_In"
    effective_out_output_path = outputFilePathPrefix + "Effective_Out"
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

  private def doDfJoinAndWriteOutput(df1: DataFrame, df2: DataFrame, joinExpr: Column, joinType: String, partitionNumber: Int, outputFilePath: String, writeOutputFlag: Boolean): DataFrame = {
    val joinedDf: DataFrame = df1.join(df2, joinExpr, joinType)

    if (writeOutputFlag) writeOutput(joinedDf, partitionNumber, outputFilePath)

    joinedDf
  }

  private def createDfFromTabFile(spark: SparkSession, filePath: String): DataFrame = {
    val df = spark.read.option("header", true).option("delimiter", "\t").csv(filePath)

    df
  }

  private def createMastDf(spark: SparkSession): DataFrame = {
    val mastDF_orig = createDfFromTabFile(spark, mastInputPath)

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

    mastDF
  }

  private def createStasDf(spark: SparkSession): DataFrame = {
    val stasDF_orig = createDfFromTabFile(spark, stasInputPath)

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

    stasDF
  }

  private def createStkoDf(spark: SparkSession): DataFrame = {
    val stkoDF_orig = createDfFromTabFile(spark, stkoInputPath)

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

    stkoDF
  }

  private def createMarcDf(spark: SparkSession): DataFrame = {
    val marcDF = createDfFromTabFile(spark, marcInputPath)

    marcDF
  }

  private def runSapBomTemplate(spark: SparkSession): Unit = {
    import spark.implicits._
    import spark.sqlContext.implicits._

    // Create DataFrames based on the content of a CSV file
    var mastDF = createMastDf(spark)
    var stasDF = createStasDf(spark)
    var stkoDF = createStkoDf(spark)
    var marcDF = createMarcDf(spark)

    println("Filter STAS records with blank DATUV")
    stasDF = stasDF.where(stasDF("STAS_DATUV") =!= "")

    println("Filter STKO records with null BMENG")
    stkoDF = stkoDF.where(stkoDF("STKO_BMENG") =!= 0)

    println("Left outer join: MAST - STAS")
    val mast_stas_leftDF = doDfJoinAndWriteOutput(mastDF, stasDF, mastDF("MAST_STLAL") === stasDF("STAS_STLAL") && mastDF("MAST_STLNR") === stasDF("STAS_STLNR"), "leftouter",
      1, mast_stas_leftouter_output_path, false)

    println("Left outer join: MAST - STAS - STKO")
    val mast_stas_stko_leftDF = doDfJoinAndWriteOutput(mast_stas_leftDF, stkoDF, mast_stas_leftDF("MAST_STLAL") === stkoDF("STKO_STLAL") && mast_stas_leftDF("MAST_STLNR") === stkoDF("STKO_STLNR"), "leftouter",
      1, mast_stas_stko_leftouter_output_path, false)

    println("Filter records: get successfully joined records")
    var mast_stas_stko_innerDf = mast_stas_stko_leftDF
      .where(mast_stas_stko_leftDF("MAST_STLAL") === mast_stas_stko_leftDF("STAS_STLAL")
        && mast_stas_stko_leftDF("MAST_STLNR") === mast_stas_stko_leftDF("STAS_STLNR")
        && mast_stas_stko_leftDF("MAST_STLAL") === mast_stas_stko_leftDF("STKO_STLAL")
        && mast_stas_stko_leftDF("MAST_STLNR") === mast_stas_stko_leftDF("STKO_STLNR"))
    // writeOutput(mast_stas_stko_innerDf, 1, mast_stas_stko_inner_output_path)

    println("Filter records: get inner join rejects")
    val mast_stas_stko_innerJoinRejectsDF = mast_stas_stko_leftDF
      .where(mast_stas_stko_leftDF("STAS_STLAL").isNull
        || mast_stas_stko_leftDF("STAS_STLNR").isNull
        || mast_stas_stko_leftDF("STKO_STLAL").isNull
        || mast_stas_stko_leftDF("STKO_STLNR").isNull)
    writeOutput(mast_stas_stko_innerJoinRejectsDF, 1, mast_stas_stko_inner_join_rejects_output_path)

    println("Add ResetID column")
    val mast_stas_stko_w_resetId = mast_stas_stko_innerDf.withColumn(
      "ResetID",
      concat(col("MAST_STLNR"), lit("_"), col("MAST_STLAL"), lit("_"), col("MAST_WERKS"), lit("_"), col("STAS_STLKN")))
    // writeOutput(mast_stas_stko_w_resetId, 1, mast_stas_stko_inner_output_path)

    println("Window aggregations!")
    val windowSpec = Window
      .orderBy(col("ResetID").asc, col("MAST_STLNR").asc, col("MAST_STLAL").asc, col("MAST_WERKS").asc, col("STAS_STLKN").asc, col("STAS_DATUV").desc, col("STAS_LKENZ").desc)

    val df1 = mast_stas_stko_w_resetId.withColumn("LagResetID", lag("ResetID", 1).over(windowSpec))
    df1.show()
    val df2 = df1.withColumn("LagLKENZ", lag("STAS_LKENZ", 1).over(windowSpec))
    val mast_stas_stko_w_lagFunc = df2.withColumn("LagDATUV", lag("STAS_DATUV", 1).over(windowSpec))
    // writeOutput(mast_stas_stko_w_lagFunc, 1, mast_stas_stko_inner_output_path)

    println("Generate ChangeEffectiveOutDate...")
    val mast_stas_stko_w_changeEffectiveDate = mast_stas_stko_w_lagFunc
      .withColumn("ChangeEffectiveDate",
        when(col("ResetID") === col("LagResetID") && col("STAS_LKENZ") === "X", col("LagDATUV")).otherwise("20380101"))
    writeOutput(mast_stas_stko_w_changeEffectiveDate, 1, mast_stas_stko_inner_output_path)

    println("Generate Effective In & Effective Out dates...")
    println("Inner join: MAST - STAS - MARC")
    val mast_stas_innerDf = doDfJoinAndWriteOutput(mastDF, stasDF, mastDF("MAST_STLAL") === stasDF("STAS_STLAL") && mastDF("MAST_STLNR") === stasDF("STAS_STLNR"), "inner", 1, mast_stas_inner_output_path, false)
    val mast_stas_marc_innerDf = doDfJoinAndWriteOutput(mast_stas_innerDf, marcDF, mastDF("MAST_MATNR") === marcDF("MATNR") && mastDF("MAST_WERKS") === marcDF("WERKS"), "inner", 1, mast_stas_marc_inner_output_path, false)

    val effectiveInDF = mast_stas_marc_innerDf.where(mast_stas_marc_innerDf("STAS_LKENZ").isNull)
    // writeOutput(effectiveInDF, 1, effective_in_output_path)

    val effectiveOutDF = mast_stas_marc_innerDf.where(mast_stas_marc_innerDf("STAS_LKENZ").isNotNull)
    // writeOutput(effectiveOutDF, 1, effective_out_output_path)

    println("GroupBy...")
    val effectiveInDfGrouped = effectiveInDF.groupBy("MAST_STLNR", "STAS_STLKN", "MAST_STLAL", "MAST_WERKS")
      .agg(min("STAS_AENNR").as("stasAENNR"), min("STAS_DATUV").as("stasDATUV"))
    writeOutput(effectiveInDfGrouped, 1, effective_in_output_path)

    val effectiveOutDfGrouped = effectiveOutDF.groupBy("MAST_STLNR", "STAS_STLKN", "MAST_STLAL", "MAST_WERKS")
      .agg(max("STAS_STASZ").as("stasSTASZ"))
    writeOutput(effectiveOutDfGrouped, 1, effective_out_output_path)
  }

}
