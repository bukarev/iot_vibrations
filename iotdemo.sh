export SPARK_HOME=/usr/hdp/current/spark2-client
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --executor-memory 4G --num-executors 6 --class azureSensortag --jars "/opt/sensortag/lib/akka-actor_2.11-2.5.15.jar,/opt/sensortag/lib/akka-protobuf_2.11-2.5.12.jar,/opt/sensortag/lib/akka-stream_2.11-2.5.12.jar,/opt/sensortag/lib/bcmail-jdk15on-1.53.jar,/opt/sensortag/lib/bcpkix-jdk15on-1.53.jar,/opt/sensortag/lib/bcprov-jdk15on-1.53.jar,/opt/sensortag/lib/cachecontrol_2.11-1.1.3.jar,/opt/sensortag/lib/commons-codec-1.6.jar,/opt/sensortag/lib/config-1.3.3.jar,/opt/sensortag/lib/gson-2.5.jar,/opt/sensortag/lib/iot-deps-0.6.2.jar,/opt/sensortag/lib/iot-deps-0.7.0.jar,/opt/sensortag/lib/iot-service-client-1.14.1.jar,/opt/sensortag/lib/javax.inject-1.jar,/opt/sensortag/lib/javax.json-1.0.4.jar,/opt/sensortag/lib/lift-json_2.11-3.3.0-RC1.jar,/opt/sensortag/lib/macro-compat_2.11-1.1.1.jar,/opt/sensortag/lib/org.eclipse.paho.client.mqttv3-1.2.0.jar,/opt/sensortag/lib/play-ahc-ws-standalone_2.11-2.0.0-M3.jar,/opt/sensortag/lib/play-functional_2.11-2.6.9.jar,/opt/sensortag/lib/play-json_2.11-2.6.9.jar,/opt/sensortag/lib/play-ws-standalone_2.11-2.0.0-M3.jar,/opt/sensortag/lib/play-ws-standalone-json_2.11-2.0.0-M3.jar,/opt/sensortag/lib/proton-j-0.25.0.jar,/opt/sensortag/lib/reactive-streams-1.0.2.jar,/opt/sensortag/lib/scala-java8-compat_2.11-0.8.0.jar,/opt/sensortag/lib/scala-library.jar,/opt/sensortag/lib/scalap-2.11.0.jar,/opt/sensortag/lib/scala-parser-combinators_2.11-1.1.0.jar,/opt/sensortag/lib/scala-xml_2.11-1.0.1.jar,/opt/sensortag/lib/shaded-asynchttpclient-2.0.0-M3.jar,/opt/sensortag/lib/shaded-oauth-2.0.0-M3.jar,/opt/sensortag/lib/ssl-config-core_2.11-0.2.3.jar,/usr/hdp/2.6.5.0-292/spark2/jars/spark-sql-kafka-0-10_2.11-2.3.0.2.6.5.0-292.jar,/usr/hdp/2.6.5.0-292/spark2/jars/kafka-clients-1.0.0.2.6.5.0-292.jar" /home/rbukarev/iot/iotdemo_hdp_noml/target/scala-2.11/iot-pm-demo-no-ml_2.11-0.0.1.jar "kafkabroker1.com.au:6667,kafkabroker2.com.au:6667,kafkabroker3.com.au:6667" "sensortag_raw" "$Default" latest 2 2000 L 247189586081 "http://sapsystemxxxxx.com.au:50000/iot1/thresholds?sap-client=100" vibration sapuser sappassword
