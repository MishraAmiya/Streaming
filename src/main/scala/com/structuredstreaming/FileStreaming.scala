/*
package com.structuredstreaming

import java.io.File

import com.univocity.parsers.csv.CsvWriterSettings
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by AMIYA on 6/2/2018.
  */
object FileStreaming {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    val structType = StructType(Array(StructField("col1",StringType,true),StructField("col2",StringType,true),StructField("col3",StringType,true)))
    val df:DataFrame = sparkSession.readStream.format("csv").option("delimiter",",").schema(structType).csv("D:\\Streaming\\src\\main\\resources\\csv")
    val file = new File("D:\\Streaming\\intermediate")
    val cws:CsvWriterSettings = new CsvWriterSettings()

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new CsvOutputWriter(path, dataSchema, context, csvOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".csv" + CodecStreams.getCompressionExtension(context)
      }
    }
    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = {
        private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

        private val gen = new UnivocityGenerator(structType, writer, params)
        true
      }

      override def process(value: Row): Unit =
      {
//        println(value)
//        new CsvWriter(file,"UTF-8",cws).writeRow(value)

      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    df.writeStream.foreach(writer).start()
    df.writeStream.format("csv").option("checkpointLocation","D:\\Streaming\\tmp").start("D:\\Streaming\\output").awaitTermination()
  }

}
*/
