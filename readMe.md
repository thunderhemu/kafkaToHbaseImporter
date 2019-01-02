Project :

it consumes the XML data from kafka and writes it into HDFS as well as Hbase

it takes config file as argument.
config description.
KAFKA_TOPIC (Mandatory) --> we have to pass kafka topic name
KAFKA_BROKER (Mandatory --> we have  to provide Kafka broker ID,It's a mediator between Kafka topic and consumer group
HDFS_DIR (Mandatory) --> We need to provide HDFS directory to store the data
WINTERVAL (Mandatory) --> We have to specify window interval, how frequent message has to be consumed
CONSUMER_GROUP_ID (Mandatory) -->We have to provide consumer group ID to receive data from Kafka topic
IS_USING_SASL (Mandatory) --> We are using Simple Authentication and Security Layer
TRUSTSTORELOCATION (Mandatory) --> We have to provide Trust Store Location
TRUSTSTOREPASSWORD (Mandatory) --> We have to provide Trust Store Password
KEYSTORELOCATION (Mandatory) --> We have to provide Key Store Location


