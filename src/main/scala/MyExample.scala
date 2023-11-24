package example

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object MyExample {
  def main(args: Array[String]): Unit = {

    //

    println("------- MyExample ------- ")

    //

    println("START MY JAR")

    // expiration date : 9 Dec 2023

    val containerName = "container2"
    val storageAccountName = "storageaccount8238"
    val sas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-09T10:24:39Z&st=2023-11-24T02:24:39Z&spr=https&sig=JjS98sKkn1VVBFlaDcD7VuoFUhBGCaSxyJAG9OVoIDE%3D"
    val config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

    //

    println("START MOUNT")

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
        source = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/",
        mountPoint = s"/mnt/$containerName",
        extraConfigs = Map(config -> sas))
      println(s"mounted /mnt/$containerName")
    }
    println(mounts.size)
    println(dbutils.fs.ls(s"/mnt/$containerName"))

    //

    println("CREATE SPARK INSTANCE")

    val spark = SparkSession.builder().getOrCreate()

    //

    val fileName1 = "userdata1.parquet"
    val fileName2 = "userdata2.parquet"
    val fileName3 = "userdata3.parquet"
    val fileName4 = "userdata4.parquet"
    val fileName5 = "userdata5.parquet"

    //

    println("START READ DATA FROM FILES")

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

    println("REMOVE DUPLICATES ROWS")

    val df1 = renameColumn(mydf1, fileName1)
    val df2 = renameColumn(mydf2, fileName2)
    val df3 = renameColumn(mydf3, fileName3)
    val df4 = renameColumn(mydf4, fileName4)
    val df5 = renameColumn(mydf5, fileName5)

    removeDuplicatesRows(df1, fileName1)
    removeDuplicatesRows(df2, fileName2)
    removeDuplicatesRows(df3, fileName3)
    removeDuplicatesRows(df4, fileName4)
    removeDuplicatesRows(df5, fileName5)


    //

    println("END MY JAR")

    //


    println("------- MyExample ------- ")


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

  private def checkMatchingOnColumnsNames(df: DataFrame, fileName: String, expectedSchema: StructType): Boolean = {
    if (df.columns.toSet == expectedSchema.fieldNames.toSet) {
      println(s"$fileName columns names matched")
    } else {
      println(s"$fileName columns names not matched")
    }
    return (df.columns.toSet == expectedSchema.fieldNames.toSet)
  }

  private def checkDataTypesMatchingOnColumns(df: DataFrame, fileName: String): Boolean = {
    println(s"CHECKING ON FILE '$fileName' ...")
    var hasExistColumnDataTypeNotMatch = false
    if (df.schema("registration_dttm").dataType.typeName == "timestamp") {
      println("registration_dttm is 'timestamp' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("id").dataType.typeName == "integer") {
      println("id is 'integer' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("salary").dataType.typeName == "double") {
      println("salary is 'double' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("first_name").dataType.typeName == "string") {
      println("first_name is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("last_name").dataType.typeName == "string") {
      println("last_name is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("email").dataType.typeName == "string") {
      println("email is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("gender").dataType.typeName == "string") {
      println("gender is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("ip_address").dataType.typeName == "string") {
      println("ip_address is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("cc").dataType.typeName == "string") {
      println("cc is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("country").dataType.typeName == "string") {
      println("country is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("birthdate").dataType.typeName == "string") {
      println("birthdate is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("title").dataType.typeName == "string") {
      println("title is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    if (df.schema("comments").dataType.typeName == "string") {
      println("comments is 'string' column")
    } else {
      hasExistColumnDataTypeNotMatch = true
    }

    return !hasExistColumnDataTypeNotMatch
  }

  private def checkValidationOnDataFiles(df: DataFrame, fileName: String, expectedSchema: StructType): Boolean = {
    val result = (checkMatchingOnColumnsNames(df, fileName, expectedSchema) && checkDataTypesMatchingOnColumns(df, fileName))
    if (result) {
      println(s"$fileName valid")
    } else {
      println(s"$fileName not valid")
    }
    return result
  }

  private def renameColumn(df: DataFrame, fileName: String): DataFrame = {
    println(s"rename column , add metadata for DF from file name : $fileName")
    val dfModified = df.withColumnRenamed("cc", "cc_mod").withMetadata("cc_mod", Metadata.fromJson("{\"tag\": \"this column has been modified\"}"))
    dfModified.printSchema()
    return dfModified
  }

  private def removeDuplicatesRows(df: DataFrame, fileName: String): Unit = {
    println(s"remove duplicates rows in DF from file name : $fileName")
    val resultDataSet = df.distinct()
    resultDataSet.show()
  }
}