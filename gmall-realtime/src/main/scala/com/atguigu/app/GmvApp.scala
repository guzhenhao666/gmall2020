package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

object GmvApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)

    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {


      val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])


      val create_time: String = info.create_time
      val dateTime: Array[String] = create_time.split(" ")
      info.create_date = dateTime(0)
      info.create_hour = dateTime(1).split(":")(0)


      val consignee_tel: String = info.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      info.consignee_tel = s"${tuple._1}*******"

      info
    })


    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
