package foreachSink

import org.apache.spark.sql.ForeachWriter
//import com.mapr.db.spark.sql._
import org.apache.spark.sql._

// ====== Imports for Cloud-To-Device messages
import com.microsoft.azure.sdk.iot.service.ServiceClient
import com.microsoft.azure.sdk.iot.service.Message
import com.microsoft.azure.sdk.iot.service.IotHubServiceClientProtocol
import com.microsoft.azure.sdk.iot.service.DeliveryAcknowledgement
//import java.util.concurrent.CompletableFuture
// ====== End of Imports for Cloud-To-Device messages

import scala.util.parsing.json.JSONObject
import org.apache.spark.sql.functions.to_json

import httpWrapper._
import KafkaAzure._

case class thresholds(DEVICEID: String, ACCX: String, ACCY: String, ACCZ: String, GYROX: String, GYROY: String, GYROZ: String )

   class AzureSink(url: String, topicOut: String, th: Array[thresholds], sapUser: String, sapPwd: String) extends ForeachWriter[Row] {
   
   // Connection string for IoT Device
   val connStr = "HostName=DDS-AU-IOT-HUB1.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=QN0t+mDpn6v3KRmIWhqFE9NXu4wy9rU6yp0jowMfKlE="
   
   
   var serviceClient:ServiceClient = _
   var sapS:sapSession = _
   var kAS:KafkaAzureSink = _
      
                                     def open(partitionId: Long, version: Long): Boolean = {
                           
                                       // open connection
                                                serviceClient = ServiceClient.createFromConnectionString(connStr, IotHubServiceClientProtocol.AMQPS);
                                                serviceClient.open()
                                                val future = serviceClient.openAsync()
                                                future.get()
                                                
                                                sapS = new sapSession(url, sapUser, sapPwd)
                                                sapS.init()
                                                sapS.call_get()
                                                
                                                kAS = new KafkaAzureSink(topicOut)
                                                true    
                                              }

                                      def process(record: Row): Unit = {
                                                    // write string to connection
                                                                      
                                                     record match {

                                                         case Row(deviceid: String, utcTime: String, gyroX: Double, gyroY: Double, gyroZ: Double, accX: Double, accY: Double, accZ: Double, outData: String) => {
                                                             
                                                              
                                                             kAS.send(outData) // push raw output to the Azure Events Hub
                                                             
                                                             if (
                                                                 Math.abs(accX - th(0).ACCX.toDouble) > th(2).ACCX.toDouble ||
                                                                 Math.abs(accY - th(0).ACCY.toDouble) > th(2).ACCY.toDouble ||
                                                                 Math.abs(accZ - th(0).ACCZ.toDouble) > th(2).ACCZ.toDouble ||
                                                                 Math.abs(gyroX - th(0).GYROX.toDouble) > th(2).GYROX.toDouble ||
                                                                 Math.abs(gyroY - th(0).GYROY.toDouble) > th(2).GYROY.toDouble ||
                                                                 Math.abs(gyroZ - th(0).GYROZ.toDouble) > th(2).GYROZ.toDouble
                                                                )  
                                                                 {
                                                                      val al2msg = utcTime + 
                                                                      ": gyroDX" + Math.abs(gyroX - th(0).GYROX.toDouble) + " vs " + th(2).GYROX.toDouble +
                                                                      ": gyroDY" + Math.abs(gyroY - th(0).GYROY.toDouble) + " vs " + th(2).GYROY.toDouble +
                                                                      ": gyroDZ" + Math.abs(gyroZ - th(0).GYROZ.toDouble) + " vs " + th(2).GYROZ.toDouble +
                                                                      ", accDX: " + Math.abs(accX - th(0).ACCX.toDouble) + " vs " + th(2).ACCX.toDouble +
                                                                      ", accDY: " + Math.abs(accY - th(0).ACCY.toDouble) + " vs " + th(2).ACCY.toDouble +
                                                                      ", accDZ: " + Math.abs(accZ - th(0).ACCZ.toDouble) + " vs " + th(2).ACCZ.toDouble
                                                                      
                                                                      println(al2msg)
                                                                      println("Alert 2 for SeqNo: " + utcTime)                                                                      
                                                                      val message = new Message("2"+ " " + al2msg) // pass diagnostics to the device
                                                                      
                                                                      message.setMessageId("msg_x")
                                                                      message.setCorrelationId("corr_x")
    
                                                                      val future = serviceClient.sendAsync( deviceid , message)
                                                                      future.get()
                                                                      
                                                                      val reqBody = "{\"deviceid\":\"" + deviceid + "\", \"alert\":\"2\", \"utctime\":\"" + utcTime + "\"}"
                                                                      sapS.call_post_async(reqBody)
                                                                      
                                                              } else if (
                                                                 Math.abs(accX - th(0).ACCX.toDouble) > th(1).ACCX.toDouble ||
                                                                 Math.abs(accY - th(0).ACCY.toDouble) > th(1).ACCY.toDouble ||
                                                                 Math.abs(accZ - th(0).ACCZ.toDouble) > th(1).ACCZ.toDouble ||
                                                                 Math.abs(gyroX - th(0).GYROX.toDouble) > th(1).GYROX.toDouble ||
                                                                 Math.abs(gyroY - th(0).GYROY.toDouble) > th(1).GYROY.toDouble ||
                                                                 Math.abs(gyroZ - th(0).GYROZ.toDouble) > th(1).GYROZ.toDouble
                                                                 ) 
                                                                 {
                                                                      val al1msg = utcTime + 
                                                                      ": gyroDX" + Math.abs(gyroX - th(0).GYROX.toDouble) + " vs " + th(1).GYROX.toDouble +
                                                                      ": gyroDY" + Math.abs(gyroY - th(0).GYROY.toDouble) + " vs " + th(1).GYROY.toDouble +
                                                                      ": gyroDZ" + Math.abs(gyroZ - th(0).GYROZ.toDouble) + " vs " + th(1).GYROZ.toDouble +
                                                                      ", accDX: " + Math.abs(accX - th(0).ACCX.toDouble) + " vs " + th(1).ACCX.toDouble +
                                                                      ", accDY: " + Math.abs(accY - th(0).ACCY.toDouble) + " vs " + th(1).ACCY.toDouble +
                                                                      ", accDZ: " + Math.abs(accZ - th(0).ACCZ.toDouble) + " vs " + th(1).ACCZ.toDouble
                                                                       
                                                                      println(al1msg)
                                                                      
                                                                      println("Alert 1 for SeqNo: " + utcTime)                                                  
                                                                      val message = new Message("1"+ " " + al1msg) // pass diagnostics to the device
                                                                      
                                                                      message.setMessageId("msg_x")
                                                                      message.setCorrelationId("corr_x")
    
                                                                      val future = serviceClient.sendAsync( deviceid , message)
                                                                      future.get()
                                                                      
                                                                      val reqBody = "{\"deviceid\":\"" + deviceid + "\", \"alert\":\"1\", \"utctime\":\"" + utcTime + "\"}"
                                                                      sapS.call_post_async(reqBody)
                                                              } // of if   
                                                       }// end of case Row
                                                     
                                                     } // end of match
                                                     
                                                     
                                              } // end of process

                                     def close(errorOrNull: Throwable): Unit = {
                                            // close the connection
                                            serviceClient.close()
                                              }
} 
