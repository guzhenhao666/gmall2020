package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object SaveUserApp {
  def main(args: Array[String]): Unit = {
    //1.获取执行环境
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveUserApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka用户主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO, ssc)

    //3.将数据写入Redis
    val value: DStream[String] = kafkaDStream.map(_.value())

    value.cache()
    value.print()

    value.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //a.获取Redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历写入
        iter.foreach(userJsonStr => {
          val userInfo: UserInfo = JSON.parseObject(userJsonStr, classOf[UserInfo])
          jedisClient.set(s"UserInfo:${userInfo.id}", userJsonStr)
        })
        //c.归还连接
        jedisClient.close()

      })
    })

    //4.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
