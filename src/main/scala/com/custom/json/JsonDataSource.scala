
package com.custom.json

import java.io.InputStream
import java.net.URI

import com.com.custom.json.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileRecordReader, CombineFileSplit, FileInputFormat}
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Common functions for parsing JSON files
  */
abstract class JsonDataSource extends Serializable {
  def isSplitable: Boolean

  /**
    * Parse a [[PartitionedFile]] into 0 or more [[InternalRow]] instances
    */
  def readFile(
                conf: Configuration,
                file: PartitionedFile,
                parser: JacksonParser,
                schema: StructType): Iterator[InternalRow]

  final def inferSchema(
                         sparkSession: SparkSession,
                         inputPaths: Seq[FileStatus],
                         parsedOptions: JSONOptions): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      Some(infer(sparkSession, inputPaths, parsedOptions))
    } else {
      None
    }
  }

  protected def infer(
                       sparkSession: SparkSession,
                       inputPaths: Seq[FileStatus],
                       parsedOptions: JSONOptions): StructType
}

object JsonDataSource {
  def apply(options: JSONOptions): JsonDataSource = {
    if (options.multiLine) {
      MultiLineJsonDataSource
    } else {
      TextInputJsonDataSource
    }
  }
}

object TextInputJsonDataSource extends JsonDataSource {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    true
  }

  override def infer(
                      sparkSession: SparkSession,
                      inputPaths: Seq[FileStatus],
                      parsedOptions: JSONOptions): StructType = {
    val json: Dataset[String] = createBaseDataset(sparkSession, inputPaths)
    inferFromDataset(json, parsedOptions)
  }

  def inferFromDataset(json: Dataset[String], parsedOptions: JSONOptions): StructType = {
    val sampled: Dataset[String] = JsonUtils.sample(json, parsedOptions)
    val rdd: RDD[UTF8String] = sampled.queryExecution.toRdd.map(_.getUTF8String(0))
    JsonInferSchema.infer(rdd, parsedOptions, CreateJacksonParser.utf8String)
  }

  private def createBaseDataset(
                                 sparkSession: SparkSession,
                                 inputPaths: Seq[FileStatus]): Dataset[String] = {
    val paths = inputPaths.map(_.getPath.toString)
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        className = classOf[TextFileFormat].getName
      ).resolveRelation(checkFilesExist = false))
      .select("value").as(Encoders.STRING)
  }

  override def readFile(
                         conf: Configuration,
                         file: PartitionedFile,
                         parser: JacksonParser,
                         schema: StructType): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
    val safeParser = new FailureSafeParser[Text](
      input => parser.parse(input, CreateJacksonParser.text, textToUTF8String),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    linesReader.flatMap(safeParser.parse)
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }
}

object MultiLineJsonDataSource extends JsonDataSource {
  override val isSplitable: Boolean = {
    false
  }

  override def infer(
                      sparkSession: SparkSession,
                      inputPaths: Seq[FileStatus],
                      parsedOptions: JSONOptions): StructType = {
    val json: RDD[PortableDataStream] = createBaseRdd(sparkSession, inputPaths)
    val sampled: RDD[PortableDataStream] = JsonUtils.sample(json, parsedOptions)
    JsonInferSchema.infer(sampled, parsedOptions, createParser)
  }

  private def createBaseRdd(
                             sparkSession: SparkSession,
                             inputPaths: Seq[FileStatus]): RDD[PortableDataStream] = {
    val paths = inputPaths.map(_.getPath)
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConf())
    val conf = job.getConfiguration
    val name = paths.mkString(",")
    FileInputFormat.setInputPaths(job, paths: _*)
    new BinaryFileRDD(
      sparkSession.sparkContext,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      conf,
      sparkSession.sparkContext.defaultMinPartitions)
      .setName(s"JsonFile: $name")
      .values
  }

  private def createParser(jsonFactory: JsonFactory, record: PortableDataStream): JsonParser = {
    val path = new Path(record.getPath())
    CreateJacksonParser.inputStream(
      jsonFactory,
      CodecStreams.createInputStreamWithCloseResource(record.getConfiguration, path))
  }

  override def readFile(
                         conf: Configuration,
                         file: PartitionedFile,
                         parser: JacksonParser,
                         schema: StructType): Iterator[InternalRow] = {
    def partitionedFileString(ignored: Any): UTF8String = {
      JsonUtils.tryWithResource {
        CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath)))
      } { inputStream =>
        UTF8String.fromBytes(ByteStreams.toByteArray(inputStream))
      }
    }

    val safeParser = new FailureSafeParser[InputStream](
      input => parser.parse(input, CreateJacksonParser.inputStream, partitionedFileString),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    safeParser.parse(
      CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath))))
  }
}

class BinaryFileRDD[T](
                        @transient private val sc: SparkContext,
                        inputFormatClass: Class[_ <: StreamFileInputFormat[T]],
                        keyClass: Class[String],
                        valueClass: Class[T],
                        conf: Configuration,
                        minPartitions: Int)
  extends NewHadoopRDD[String, T](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = new JobContextImpl(conf, jobId)
    inputFormat.setMinPartitions(sc, jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}

abstract class StreamFileInputFormat[T]
  extends CombineFileInputFormat[String, T] {
  /**
    * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API
    * which is set through setMaxSplitSize
    */
  def setMinPartitions(sc: SparkContext, context: JobContext, minPartitions: Int) {
    val defaultMaxSplitBytes = Long.unbox(Try {
      sc.getConf.get("spark.files.maxPartitionBytes")
    }.getOrElse(String.valueOf(128 * 1024 * 1024)))
    val openCostInBytes = Long.unbox(Try {
      sc.getConf.get("spark.files.openCostInBytes")
    }.getOrElse(String.valueOf(4 * 1024 * 1024)))
    val defaultParallelism = sc.defaultParallelism
    val files = listStatus(context).asScala
    val totalBytes = files.filterNot(_.isDirectory).map(_.getLen + openCostInBytes).sum
    val bytesPerCore = totalBytes / defaultParallelism
    val maxSplitSize = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    super.setMaxSplitSize(maxSplitSize)
  }

  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]

  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

}

class NewHadoopPartition(
                          rddId: Int,
                          val index: Int,
                          rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

class StreamInputFormat extends StreamFileInputFormat[PortableDataStream] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)
  : CombineFileRecordReader[String, PortableDataStream] = {
    new CombineFileRecordReader[String, PortableDataStream](
      split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
  }
}

class StreamRecordReader(
                          split: CombineFileSplit,
                          context: TaskAttemptContext,
                          index: Integer)
  extends StreamBasedRecordReader[PortableDataStream](split, context, index) {

  def parseStream(inStream: PortableDataStream): PortableDataStream = inStream
}

abstract class StreamBasedRecordReader[T](
                                           split: CombineFileSplit,
                                           context: TaskAttemptContext,
                                           index: Integer)
  extends RecordReader[String, T] {

  // True means the current file has been processed, then skip it.
  private var processed = false

  private var key = ""
  private var value: T = null.asInstanceOf[T]

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: T = value

  override def nextKeyValue: Boolean = {
    if (!processed) {
      val fileIn = new PortableDataStream(split, context, index)
      value = parseStream(fileIn)
      key = fileIn.getPath
      processed = true
      true
    } else {
      false
    }
  }

  /**
    * Parse the stream (and close it afterwards) and return the value as in type T
    *
    * @param inStream the stream to be read in
    * @return the data formatted as
    */
  def parseStream(inStream: PortableDataStream): T
}