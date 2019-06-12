package KafkaAzure

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.security.plain.PlainLoginModule

import scala.collection.JavaConverters._

class KafkaAzureSink(topicOut: String) {

   var producer = new KafkaProducer[Array[Byte], Array[Byte]](
             Map[String, Object](
             "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
             "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
             "bootstrap.servers" -> "XXXXXXXX.servicebus.windows.net:9093",
             "ssl.truststore.location" -> "/var/local/kafka.client.truststore.jks",
             "ssl.truststore.password" -> "XXXXXXX",
             "sasl.mechanism" -> "PLAIN",
             "security.protocol" -> "SASL_SSL",
             "sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=CONNECTIONSTRINGHERE;"
           ).asJava)
   
   def send(recOut: String): Unit = {
           println("to Azure: " + recOut)   
           producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topicOut, recOut.getBytes, recOut.getBytes), 
                           new Callback(){
                                           override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                                         //          println(exception)
                                         //          println(metadata)
                                              }
                                         }
                          )
   }


}