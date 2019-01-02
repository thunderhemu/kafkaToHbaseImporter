package com.hemanth.core
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class Config extends LazyLogging {

  val properties: Properties = new Properties()

  def loadConfig(configPath: String, sc: SparkContext) = {
    logger.info("Loading configuration from: " + configPath)
    val path = new Path(configPath)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputStream = fs.open(path)
    properties.load(inputStream)
    logger.info("Done loading configuration.")
  }
  def get(key: String, defValue: String): String = {
    properties.getProperty(key, defValue)
  }
}


