package com.com.custom.json.catalyst.json

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
object JacksonUtils {
  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  /**
   * Verify if the schema is supported in JSON parsing.
   */
  def verifySchema(schema: StructType): Unit = {
    def verifyType(name: String, dataType: DataType): Unit = dataType match {
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | StringType | TimestampType | DateType | BinaryType | _: DecimalType =>

      case st: StructType => st.foreach(field => verifyType(field.name, field.dataType))

      case at: ArrayType => verifyType(name, at.elementType)

      // For MapType, its keys are treated as a string (i.e. calling `toString`) basically when
      // generating JSON, so we only care if the values are valid for JSON.
      case mt: MapType => verifyType(name, mt.valueType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert column $name of type ${dataType.simpleString} to JSON.")
    }

    schema.foreach(field => verifyType(field.name, field.dataType))
  }
}
abstract class UserDefinedType[UserType >: Null] extends DataType with Serializable {

  /** Underlying storage type for this UDT */
  def sqlType: DataType

  /** Paired Python UDT class, if exists. */
  def pyUDT: String = null

  /** Serialized Python UDT class, if exists. */
  def serializedPyClass: String = null

  /**
    * Convert the user type to a SQL datum
    */
  def serialize(obj: UserType): Any

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType

  override def  jsonValue: JValue = {
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> pyUDT) ~
      ("sqlType" -> sqlType.typeName)
  }

  /**
    * Class object for the UserType
    */
  def userClass: java.lang.Class[UserType]

  override def defaultSize: Int = sqlType.defaultSize

  /**
    * For UDT, asNullable will not change the nullability of its internal sqlType and just returns
    * itself.
    */
  def asNullable: UserDefinedType[UserType] = this

  override def acceptsType(dataType: DataType) = dataType match {
    case other: UserDefinedType[_] =>
      this.getClass == other.getClass ||
        this.userClass.isAssignableFrom(other.userClass)
    case _ => false
  }

  override def sql: String = sqlType.sql

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: UserDefinedType[_] => this.acceptsType(that)
    case _ => false
  }

  override def catalogString: String = sqlType.simpleString
}
