package com.jsonmain

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
  * Created by AMIYA on 8/5/2018.
  */
object json_datasource_check {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    val metadata1 = new MetadataBuilder().putString("dateformat","yyyy/MM/dd").build()
    val metadata = new MetadataBuilder().putString("dateformat","yyyy-MM-dd").build()
    val metadata2 = new MetadataBuilder().putString("dateformat","yyyyMMdd").build()
    val structType = StructType(Array(StructField("col1", ArrayType(DateType), true,metadata), StructField("col2", ArrayType(DateType), true,metadata1), StructField("r1", ArrayType(ArrayType(ArrayType(StructType(Array(StructField("col3",StringType,true),StructField("col4",ArrayType(DateType),true,metadata2)))))), true)))
    val df = sparkSession.readStream.format("com.custom.json.JsonFileFormat").schema(structType).load("D:\\Streaming\\src\\main\\resources\\jsoninput")
//        val df = sparkSession.readStream.schema(structType).json("D:\\Streaming\\src\\main\\resources\\jsoninput")
//    df.writeStream.format("console").start().awaitTermination()

    df.writeStream.format("json").option("checkpointLocation","checkpoi").option("path","out").start().awaitTermination()
  }
}
