package example

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object MyJob2 {
  def main(args: Array[String]): Unit = {

    println(" ----  MyJob2  ------ ")

    println("START MY JAR")


    val serviceCredential = dbutils.secrets.get(scope = "key-vault-secret-1", key = "secret")
    val sasCredential = dbutils.secrets.get(scope = "key-vault-secret-1", key = "sas")

    val appId = "8d16215c-9399-4c1d-8164-bbb2f9fc8a55"
    val tenantId = "e85413be-9893-4b17-ac77-83c4443a22a3"

    val containerName = "level2"
    val storageAccountName = "storageaccount8238"

    //


    println("SAS token  -  expiration date : 9 Dec 2023")


    //

    println("CREATE SPARK INSTANCE")

    val spark = SparkSession.builder().getOrCreate()

    //

    println("CONFIG SPARK")

    spark.conf.set(s"fs.azure.account.auth.type.$storageAccountName.dfs.core.windows.net", "OAuth")
    spark.conf.set(s"fs.azure.account.oauth.provider.type.$storageAccountName.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(s"fs.azure.account.oauth2.client.id.$storageAccountName.dfs.core.windows.net", appId)
    spark.conf.set(s"fs.azure.account.oauth2.client.secret.$storageAccountName.dfs.core.windows.net", serviceCredential)
    spark.conf.set(s"fs.azure.account.oauth2.client.endpoint.$storageAccountName.dfs.core.windows.net", s"https://login.microsoftonline.com/$tenantId/oauth2/token")
    spark.conf.set(s"fs.azure.account.key.$storageAccountName>.dfs.core.windows.net", sasCredential)

    //


    println("START MOUNT")

    val configs = Map(
      "fs.azure.account.auth.type" -> "OAuth",
      "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id" -> appId,
      "fs.azure.account.oauth2.client.secret" -> serviceCredential,
      "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$tenantId/oauth2/token",
      "fs.azure.createRemoteFileSystemDuringInitialization" -> "true")


    var mounts = dbutils.fs.ls("/mnt/").filter(_.name.contains(s"$containerName"))
    println(mounts.size)
    if (mounts.nonEmpty) {
      dbutils.fs.unmount(s"/mnt/$containerName")
      println(s"force unmounted /mnt/$containerName")
    }
    mounts = dbutils.fs.ls("/mnt/").filter(_.name.contains(s"$containerName"))
    println(mounts.size)
    if (mounts.isEmpty) {
      dbutils.fs.mount(
        source = s"abfss://$containerName@$storageAccountName.dfs.core.windows.net",
        mountPoint = s"/mnt/$containerName",
        extraConfigs = configs)
      println(s"mounted /mnt/$containerName")
    }
    println(mounts.size)
    println(dbutils.fs.ls(s"/mnt/$containerName"))


    //

    val fileName1 = "userdata1.parquet"
    val fileName2 = "userdata2.parquet"
    val fileName3 = "userdata3.parquet"
    val fileName4 = "userdata4.parquet"
    val fileName5 = "userdata5.parquet"

    //

    println("READ DATA FROM FILES")

    val mydf1 = readDataFromContainer(spark, containerName, fileName1)
    val mydf2 = readDataFromContainer(spark, containerName, fileName2)
    val mydf3 = readDataFromContainer(spark, containerName, fileName3)
    val mydf4 = readDataFromContainer(spark, containerName, fileName4)
    val mydf5 = readDataFromContainer(spark, containerName, fileName5)

    //

    println("CREATE EXPECTED SCHEMA")

    // The schema is encoded in a string
    var schemaString = "first_name last_name email gender ip_address cc country birthdate title comments"
    // Generate the schema based on the string of schema
    val fieldsString = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    schemaString = "id"
    val fieldsInteger = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = true))
    schemaString = "salary"
    val fieldsDouble = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, DoubleType, nullable = true))
    schemaString = "registration_dttm"
    val fieldsTimestamp = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, TimestampType, nullable = true))
    val fields = fieldsString ++ fieldsInteger ++ fieldsDouble ++ fieldsTimestamp
    val expectedSchema = StructType(fields)
    expectedSchema.printTreeString()

    //

    println("CHECK VALIDATION ON DATA FILES")

    checkValidationOnDataFiles(mydf1, fileName1, expectedSchema)
    checkValidationOnDataFiles(mydf2, fileName2, expectedSchema)
    checkValidationOnDataFiles(mydf3, fileName3, expectedSchema)
    checkValidationOnDataFiles(mydf4, fileName4, expectedSchema)
    checkValidationOnDataFiles(mydf5, fileName5, expectedSchema)

    //

    println("TRANSFORM DATA FRAME")

    val transformedDataFrame1 = transformData(mydf1, fileName1)
    val transformedDataFrame2 = transformData(mydf2, fileName2)
    val transformedDataFrame3 = transformData(mydf3, fileName3)
    val transformedDataFrame4 = transformData(mydf4, fileName4)
    val transformedDataFrame5 = transformData(mydf5, fileName5)

    //

    println("WRITE TRANSFORMED DATA TO Azure Data Lake Storage Gen2 ")


    writeDataFrameToParquetFileInContainer(transformedDataFrame1, fileName1)
    writeDataFrameToParquetFileInContainer(transformedDataFrame2, fileName2)
    writeDataFrameToParquetFileInContainer(transformedDataFrame3, fileName3)
    writeDataFrameToParquetFileInContainer(transformedDataFrame4, fileName4)
    writeDataFrameToParquetFileInContainer(transformedDataFrame5, fileName5)


    //

    println("END MY JAR")

    //


    println(" ----  MyJob2  ------ ")


    //


  }


  private def readDataFromContainer(spark: SparkSession, containerName: String, fileName: String): DataFrame = {
    println(s"read data from file name : $fileName")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(s"/mnt/$containerName/$fileName")
    df.printSchema()
    df.show()
    return df
  }


  private def checkMatchingOnColumnsNames(df: DataFrame, fileName: String, expectedSchema: StructType): Boolean = (df.columns.toSet == expectedSchema.fieldNames.toSet)


  private def checkDataTypesMatchingOnColumns(df: DataFrame, fileName: String): Boolean = (df.schema("registration_dttm").dataType.typeName == "timestamp" && df.schema("id").dataType.typeName == "integer" && df.schema("salary").dataType.typeName == "double" && df.schema("first_name").dataType.typeName == "string" && df.schema("last_name").dataType.typeName == "string" && df.schema("email").dataType.typeName == "string" && df.schema("gender").dataType.typeName == "string" && df.schema("ip_address").dataType.typeName == "string" && df.schema("cc").dataType.typeName == "string" && df.schema("country").dataType.typeName == "string" && df.schema("birthdate").dataType.typeName == "string" && df.schema("title").dataType.typeName == "string" && df.schema("comments").dataType.typeName == "string")


  private def checkValidationOnDataFiles(df: DataFrame, fileName: String, expectedSchema: StructType): Boolean = {
    val result = (checkMatchingOnColumnsNames(df, fileName, expectedSchema) && checkDataTypesMatchingOnColumns(df, fileName))
    if (result) {
      println(s"$fileName valid")
    } else {
      println(s"$fileName not valid")
    }
    return result
  }


  private def transformData(df: DataFrame, fileName: String): DataFrame = {
    println(s"transform data from file : $fileName")
    val dfModified = df.withColumnRenamed("cc", "cc_mod").withMetadata("cc_mod", Metadata.fromJson("{\"tag\": \"this column has been modified\"}"))
    val dataSet = dfModified.distinct()
    val resultDataFrame = dataSet.toDF()
    resultDataFrame.printSchema()
    return resultDataFrame
  }


  private def writeDataFrameToParquetFileInContainer(df: DataFrame, fileName: String): Unit = {
    val containerName = "level3"
    val storageAccountName = "storageaccount8238"
    val outputPath = "/FileStore/tables/output"
    df.write.mode("overwrite").parquet(outputPath)
    dbutils.fs.ls(outputPath)
    val filteredParquetFiles = dbutils.fs.ls(outputPath).filter(_.name.contains("parquet"))
    if (filteredParquetFiles.nonEmpty) {
      val resultParquetFile = filteredParquetFiles.head
      println(s"write transformed data to parquet file '$fileName' in container named '$containerName'  ")
      println(s"path :  ${resultParquetFile.path}  ")
      dbutils.fs.cp(resultParquetFile.path, s"abfss://$containerName@$storageAccountName.dfs.core.windows.net/$fileName")
    }
  }


}