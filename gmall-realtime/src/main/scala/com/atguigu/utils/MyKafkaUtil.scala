package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")

  private val broker_list: String = properties.getProperty("kafka.broker.list")
  private val groupId: String = properties.getProperty("kafka.group.id")

  var kafkaParam = Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],

    "group.id" -> groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(topic: String, ssc: StreamingContext) ={
    KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
  }

}
