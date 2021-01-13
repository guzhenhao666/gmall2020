package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT, ssc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(
      record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val dateHourStr: String = sdf.format(new Date(eventLog.ts))
        val dateHourArr: Array[String] = dateHourStr.split(" ")

        eventLog.logDate = dateHourArr(0)
        eventLog.logHour = dateHourArr(1)

        (eventLog.mid, eventLog)
      }
    )

    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.map {
      case (mid, iter) => {
        val uids: util.HashSet[String] = new util.HashSet[String]()

        val itemIds = new util.HashSet[String]()

        val events = new util.ArrayList[String]()

        var noClick: Boolean = true

        breakable {
          iter.foreach(
            log => {
              val evid: String = log.evid
              events.add(evid)
              if ("clickItem".equals(evid)) {
                noClick = false
                break()
              } else if ("coupon".equals(evid)) {
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }
            }
          )
        }

        (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    }
    val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)

    alertInfoDStream.cache()
    alertInfoDStream.print(100)

    alertInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
            val indexName = s"${GmallConstant.ES_ALERT_INDEX_PRE}-$todayStr"
            val docList: List[(String, CouponAlertInfo)] = iter.toList.map(
              alertInfo => {
                val min: Long = alertInfo.ts / 1000 / 60
                (s"${alertInfo.mid}-$min", alertInfo)
              }
            )
            MyEsUtil.insertBulk(indexName, docList)
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
