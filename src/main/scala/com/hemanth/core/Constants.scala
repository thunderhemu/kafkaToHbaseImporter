package com.hemanth.core

class Constants (config : Config){
  val KAFKA_TOPIC        = config.get("kafka.topic.name",null)
  val HBASE_TABLE_NAME   = config.get("hbase.table.name",null)
  val KAFKA_BROKER       = config.get("kafka.broker.id",null)
  val HDFS_DIR           = config.get("hadoop.storage.dir",null)
  val WTNTERVAL          = config.get("interval.period",null)
  val CONSUMER_GROUP_ID  = config.get("consumer.group.id",null)
  val IS_USING_SASL      = config.get("sasl.authunecation",null)
  val TRUSTSTORELOCATION = config.get("trusts.store.location",null)
  val TRUSTSTOREPASSWORD = config.get("trusts.store.password",null)
  val KEYSTORELOCATION   = config.get("key.store.location",null)
  val HBASE_ZOOKEEPER_QUORUM = config.get("hbase.zookeeper",null)
  val HBASE_ZOOKEEPER_PORT = config.get("hbase.zookeeper.port",null)
}

