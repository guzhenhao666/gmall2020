package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]]
    = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP,ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(
      record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val ts: Long = startUpLog.ts
        val dateHourStr: String = sdf.format(new Date(ts))
        val dateHourArr: Array[String] = dateHourStr.split(" ")

        startUpLog.logDate = dateHourArr(0)
        startUpLog.logHour = dateHourArr(1)

        startUpLog
      }
    )

    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    val filterByMidDStream = DauHandler.filterByMid(filterByRedisDStream)

    DauHandler.saveMidToRedis(filterByMidDStream)

    filterByMidDStream.foreachRDD(
      rdd=>{
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )


    ssc.start()
    ssc.awaitTermination()

  }
}
