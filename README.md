Spark structured streaming implementation of an IoT preventative maintenance scenario involving:

1. Read vibration thresholds (linear and angle accelerations) from a SAP system via REST API.
2. Read data from a Kafka topic (the topic itsef is filled by NiFi with messages from IoT Hub in Azure cloud).
3. Post data to an Azure Event Hub, to which a PowerBI realtime dashboard is attached.
4. Assess incoming data against thresholds; if the latter are exceeded, post an alert to SAP system via REST API.
