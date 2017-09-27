package knoldus

import java.sql.Timestamp

import knoldus.GlobalObject._

import scala.util.Random
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.DataFrame


object IntegratedDataFrames extends App {


  //DataFrame from batch Data
  val staticDF = spark
    .readStream
    .schema(mySchema)
    .csv("src/main/resources/")
   staticDF.printSchema()
  //staticDF.writeStream.format("console").option("truncate",false).start
  Thread.sleep(100)


  //val result = csvDF.groupBy().agg(sum("data").as("sum"))
  //result.show()*/

  val records = List.fill(10)(Random.nextInt())
  val bootStrapServers = "localhost:9092"

  val producerProperties = new Properties()
  producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExample")
  producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](producerProperties)

  import spark.implicits._

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", "dataTopic")
    .option("includeTimestamp", true)
    .load()

  val streamingDf = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select((col("value").cast("Integer")).as("data"), $"timestamp")
    .select("data", "timestamp")

//  streamingDf.printSchema()

  val myDF: DataFrame = streamingDf.union(staticDF)

  myDF.printSchema()


  //myDF.writeStream.format("console").option("truncate", false).start()

  myDF.withWatermark("timestamp","1 second")
    .groupBy(window($"timestamp", "1 second"))
    .count()
  //  .agg(sum("data").as("sum"))
    .writeStream
    .format("console")
    .option("truncate", "false")
    .start()
  Thread.sleep(10000)


  records.map(record => {
    val producerRecord = new ProducerRecord[String, String]("dataTopic", record.toString)
    producer.send(producerRecord)
    Thread.sleep(1000)
  })

  Thread.sleep(30000)

}
