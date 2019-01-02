package com.hemanth

import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolServerSideTranslatorPB
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StreamerApp extends App {
  val conf = new SparkConf().setAppName("hbasestreamer testing ").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val kafkaHbaseStreamer = new Streamer(spark)
  kafkaHbaseStreamer.process(args)
  spark.stop()
}


