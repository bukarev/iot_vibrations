import scala.util.matching.Regex

import org.apache.spark._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.kafka010.KafkaSourceRDDPartition
import org.apache.spark.sql.types._

//import com.mapr.db.spark._
//import com.mapr.db.spark.sql._
import org.apache.spark.sql._
//import com.mapr.db.spark.streaming._

import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.functions.typedLit

import httpWrapper._
import foreachSink._

object azureSensortag {
  
  def main(args: Array[String]) {

       case class iotSignal(deviceId: String, msgID: String, utctime: String, gyroX: String, gyroY: String, gyroZ: String, accX: String, accY: String, accZ: String)   
    
       val Array(
              brokers,
              topics,
              groupId,
              offsetReset,
              batchInterval,
              pollTimeout,
              mode,
              deviceid,
              sapURL,
              topicOut,
              sapUser,
              sapPwd) = args

  val alert_url = "http://sapsystemXXXXXXXX.com.au:50000/iot1/alerts?sap-client=100"

       val topicsSet = topics.split(",").toSet

       val spark = SparkSession
         .builder
         .appName("azureSensorTag")
         .getOrCreate()

       import spark.implicits._
       
       spark.sparkContext.setLogLevel("WARN")
// ======= Obtain thresholds from SAP System

   val sapS = new sapSession(sapURL, sapUser, sapPwd)
   sapS.init()
   sapS.call_get()
   val reqBody = "{\"deviceid\":\"" + deviceid + "\"}"
   val body = sapS.call_post_sync(reqBody)
   
   implicit val formats = DefaultFormats
   val myThresholds = parse(body).extract[Array[thresholds]]
   
   println(myThresholds(0).DEVICEID + " " + myThresholds(0).ACCX + " " + myThresholds(0).ACCY + " " + myThresholds(0).ACCZ + " " + myThresholds(0).GYROX + " " + myThresholds(0).GYROY + " " + myThresholds(0).GYROZ)
   println(myThresholds(1).DEVICEID + " " + myThresholds(1).ACCX + " " + myThresholds(1).ACCY + " " + myThresholds(1).ACCZ + " " + myThresholds(1).GYROX + " " + myThresholds(1).GYROY + " " + myThresholds(1).GYROZ)
   println(myThresholds(2).DEVICEID + " " + myThresholds(2).ACCX + " " + myThresholds(2).ACCY + " " + myThresholds(2).ACCZ + " " + myThresholds(2).GYROX + " " + myThresholds(2).GYROY + " " + myThresholds(2).GYROZ)
    
// ======== Got thresholds into myThresholds
    
// ======== Start Spark Streaming job

        val messages = spark.readStream
                   .format("kafka").
                   option("subscribe", topics).
                   option("kafka.bootstrap.servers", brokers).
                   option("group.id", groupId).
                   option("auto.offset.reset", offsetReset).
                   option("failOnDataLoss", "false").
                   load
   
    val messages1 = spark.readStream
                   .format("kafka").
                   option("subscribe", topics).
                   option("kafka.bootstrap.servers", brokers).
                   option("group.id", groupId).
                   option("auto.offset.reset", offsetReset).
                   option("failOnDataLoss", "false").
                   load               

    val jDeviceId = "deviceId=(.*?),".r
    val jOffset = "offset=(.*?),".r
    val jEnqueuedTime = "enqueuedTime=(.*?),".r
    val jSequenceNumber = "sequenceNumber=(.*?),".r
    val jContent = "content=(.*?)},".r

    //implicit val formats = DefaultFormats

    val getOffset = udf((x: String) => (jOffset findFirstIn x).mkString.replace("offset=","").dropRight(1))
    val getEnqTime = udf((x: String) => (jEnqueuedTime findFirstIn x).mkString.replace("enqueuedTime=","").dropRight(1))
    val getSeqNumber = udf((x: String) => (jSequenceNumber findFirstIn x).mkString.replace("sequenceNumber=","").dropRight(1).toInt)
    val getContent = udf((x: String) => (jContent findFirstIn x).mkString.replace("content=","").dropRight(1))


    val getDeviceId = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].deviceId.toString.replace(":","") })
    val getGyroX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroX.toDouble }) 
    val getGyroY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroY.toDouble })
    val getGyroZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroZ.toDouble })
    val getAccX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accX.toDouble }) 
    val getAccY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accY.toDouble })
    val getAccZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accZ.toDouble })
    val getMsgID = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].msgID })
    val getUTCTime = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].utctime.toString })

    val outAnalyze = messages1
                        .filter($"topic".equalTo(typedLit("sensortag_raw")))
                        .select($"value" cast "string")
                        .withColumn("deviceId", getDeviceId(col("value")))
                        .withColumn("utcTime", getUTCTime(col("value")))
                        .withColumn("gyroX", getGyroX(col("value")))
                        .withColumn("gyroY", getGyroY(col("value")))
                        .withColumn("gyroZ", getGyroZ(col("value")))
                        .withColumn("accX", getAccX(col("value")))
                        .withColumn("accY", getAccY(col("value")))
                        .withColumn("accZ", getAccZ(col("value")))
                        .withColumn("outData", to_json(struct($"deviceId", $"utcTime", $"gyroX", $"gyroY", $"gyroZ", $"accX", $"accY", $"accZ")))
                        .select("deviceId", "utcTime", "gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ", "outData")
                        .writeStream
                        .outputMode("append")
                        .foreach(new AzureSink(alert_url, topicOut, myThresholds, sapUser, sapPwd))
                        .start()                                                                                      
                        
    val outConsole = messages
                        .filter($"topic".equalTo(typedLit("sensortag_raw")))
                        .select($"value" cast "string")
                        .withColumn("deviceId", getDeviceId(col("value")))
                        .withColumn("utcTime", getUTCTime(col("value")))
                        .withColumn("gyroX", getGyroX(col("value")))
                        .withColumn("gyroY", getGyroY(col("value")))
                        .withColumn("gyroZ", getGyroZ(col("value")))
                        .withColumn("accX", getAccX(col("value")))
                        .withColumn("accY", getAccY(col("value")))
                        .withColumn("accZ", getAccZ(col("value")))
                        .withColumn("outData", to_json(struct($"deviceId", $"utcTime", $"gyroX", $"gyroY", $"gyroZ", $"accX", $"accY", $"accZ")))
                        .select("deviceId", "utcTime", "gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ")
                        .writeStream
                        .outputMode("append")
                        .format("console")
                        .start()
                        
     outConsole.awaitTermination()                    
     outAnalyze.awaitTermination()
     
  } // of main()
  
}
