package com.custom.json

import java.io.{Closeable, IOException}

import com.com.custom.json.catalyst.json.JSONOptions
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.util.control.NonFatal


object JsonUtils {
  /**
    * Sample JSON dataset as configured by `samplingRatio`.
    */
  def sample(json: Dataset[String], options: JSONOptions): Dataset[String] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      json
    } else {
      json.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  /**
    * Sample JSON RDD as configured by `samplingRatio`.
    */
  def sample(json: RDD[PortableDataStream], options: JSONOptions): RDD[PortableDataStream] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      json
    } else {
      json.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw new RuntimeException("Exception encountered", e)

      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
}
