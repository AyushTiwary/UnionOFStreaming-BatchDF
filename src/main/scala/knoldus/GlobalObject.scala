package knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object GlobalObject {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("Spark-Batch-Streaming-Analytics")
    .master("local")
    .getOrCreate()

  val dirPath: String = "src/main/resources/"

  val mySchema = StructType(Array(
    StructField("data", IntegerType),
    StructField("timestamp", TimestampType)
  ))

}
