package com.hemanth

import java.text.SimpleDateFormat
import java.util.Calendar

import com.databricks.spark.xml.XmlReader
import com.kroger.core.{Config, Constants}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class Streamer(spark: SparkSession) extends LazyLogging {


  def process(args: Array[String]): Unit = {
    if (args.length != 1)
      throw new IllegalArgumentException(" Invalid number of arguments, the application accepts one arguments ")
    val config = new Config()
    config.loadConfig(args(0), spark.sparkContext)
    val constants = new Constants(config)
    validation(constants)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(constants.WTNTERVAL.toInt))
    val commonParams = Map[String, Object](
      "bootstrap.servers" -> constants.KAFKA_BROKER,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> constants.CONSUMER_GROUP_ID,
      "auto.offset.reset" -> "latest", //"earliest", "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //(true: java.lang.Boolean), //
      // "security.protocol" -> (if (constants.IS_USING_SASL.toLowerCase == "true") "SASL_SSL" else "SASL_PLAINTEXT" )
    )

    val additionalSslParams = if (constants.IS_USING_SASL.toLowerCase == "true") {
      Map(
        "ssl.truststore.location" -> constants.TRUSTSTORELOCATION,
        "ssl.truststore.password" -> constants.TRUSTSTOREPASSWORD,
        "ssl.keystore.location" -> constants.KEYSTORELOCATION,
        "ssl.keystore.password" -> constants.TRUSTSTOREPASSWORD
      )
    } else {
      Map.empty
    }

    val kafkaParams = commonParams ++ additionalSslParams

    val topicsSet = constants.KAFKA_TOPIC.split(",").toSet
    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
      topicsSet,
      kafkaParams))
    try {
      kafkaStream.foreachRDD(r => {
        //get off set of read messages
        val offsetRanges = r.asInstanceOf[HasOffsetRanges].offsetRanges
        val rawRdd = r.map(x => x.value()).filter(x => x != null)
        val filteredRdd = rawRdd.filter(x => x.length > 5)
        val xmlDf = new XmlReader().xmlRdd(spark.sqlContext, filteredRdd)
        val dateFormat = new SimpleDateFormat("MM-dd-YYYY")
        val ct = Calendar.getInstance().getTime()
        val currentDate = dateFormat.format(ct)
        val finaleDf = xmlDf.withColumn("datestamp", lit(currentDate))
        finaleDf.show(10)
        if (finaleDf.take(2).length > 0) {
          val colArrayStr = finaleDf.columns.mkString(",")
          val writeDF = finaleDf.selectExpr("customerid", "concat(" + colArrayStr + ") as message ", "datestamp")
          // wrting into hdfs
          writeDF.write.partitionBy("datestamp").mode(SaveMode.Append).parquet(constants.HDFS_DIR)
          val hbaseDf = (writeDF.selectExpr("concat('key_',customerid) as key", "cast(customerid as string) as c", "message as m",
            "cast(datestamp as string) as d")).na.fill("NA").filter("key is not null")
          val catalogRead =
            s"""{"table":{"namespace":"default", "name":"${constants.HBASE_TABLE_NAME}"},
                                     "rowkey":"key",
                                     "columns":{
                                     "key":{"cf":"rowkey", "col":"key", "type":"string"},
                                     "c":{"cf":"d", "col":"c", "type":"String"},
                                     "m":{"cf":"d", "col":"m", "type":"String"},
                                     "d":{"cf":"d", "col":"d", "type":"string"} }}""".stripMargin
          hbaseDf.show(100, false)
          hbaseDf.write.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead, HBaseTableCatalog.newTable -> "5")).
            format("org.apache.spark.sql.execution.datasources.hbase").save()
        }
        // commit off set back to kafka
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
    }
    catch {
      case e: Exception => println(e)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
    ssc.start()
    ssc.awaitTermination()
  }

  // validates config file
  def validation(constants: Constants): Unit = {
    if (constants.KAFKA_BROKER == null)
      throw new IllegalArgumentException(" Invalid kafka broker, kafka broker can't be null!! ")
    if (constants.KAFKA_TOPIC == null)
      throw new IllegalArgumentException(" Invalid kafka topic, kafka topic can't be null!! ")
    if (constants.WTNTERVAL == null)
      throw new IllegalArgumentException(" Invalid window interval, window interval can't be null!! ")
    if (constants.HDFS_DIR == null)
      throw new IllegalArgumentException(" Invalid hdfs directory, hdfs directory can't be null!! ")
    if (constants.HBASE_TABLE_NAME == null)
      throw new IllegalArgumentException(" Invalid HBase table, HBase table can't be null!! ")
    if (constants.CONSUMER_GROUP_ID == null)
      throw new IllegalArgumentException(" Invalid consumer group id, consumer group id can't be null!! ")
    if (constants.IS_USING_SASL == null)
      throw new IllegalArgumentException(" Invalid SASL flag, SASL flag can't be null!! ")
    if (constants.IS_USING_SASL.toLowerCase != "true" && constants.IS_USING_SASL.toLowerCase != "false")
      throw new IllegalArgumentException(" Invalid SASL flag, SASL flag can be either true or false!! ")
    if (constants.IS_USING_SASL.toLowerCase == "true" && constants.TRUSTSTOREPASSWORD == null)
      throw new IllegalArgumentException(" Invalid truststorepassword, SASL flag set to true, truststorepassword can't be null!! ")
    if (constants.IS_USING_SASL.toLowerCase == "true" && constants.TRUSTSTORELOCATION == null)
      throw new IllegalArgumentException(" Invalid truststorelocation, SASL flag set to true, truststorelocation can't be null!! ")
    if (constants.IS_USING_SASL.toLowerCase == "true" && constants.KEYSTORELOCATION == null)
      throw new IllegalArgumentException(" Invalid KeyStoreLocation, SASL flag set to true, KeystoreLocation can't be null!! ")
  }
}

