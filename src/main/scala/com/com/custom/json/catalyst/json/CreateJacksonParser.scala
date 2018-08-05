

package com.com.custom.json.catalyst.json

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.io.Text
import org.apache.spark.unsafe.types.UTF8String

 object CreateJacksonParser extends Serializable {
  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def utf8String(jsonFactory: JsonFactory, record: UTF8String): JsonParser = {
    val bb = record.getByteBuffer
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    jsonFactory.createParser(new InputStreamReader(bain, "UTF-8"))
  }

  def text(jsonFactory: JsonFactory, record: Text): JsonParser = {
    jsonFactory.createParser(record.getBytes, 0, record.getLength)
  }

  def inputStream(jsonFactory: JsonFactory, record: InputStream): JsonParser = {
    jsonFactory.createParser(record)
  }
}
