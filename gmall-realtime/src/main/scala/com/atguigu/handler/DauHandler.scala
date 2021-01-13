package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startLog => {
      ((startLog.logDate, startLog.mid), startLog)
    })
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    dateMidToLogIterDStream.flatMap {
     case ((date,mid),iter)=>{
       iter.toList.sortWith(_.ts < _.ts).take(1)
     }
    }

  }


  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sparkContext: SparkContext) = {
    //方案一：使用filter算子实现去重功能
    //    startUpLogDStream.filter(
    //      startUpLog=>{
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        val redisKey = s"DAU:${startUpLog.logDate}"
    //        val exist: lang.Boolean = jedisClient.sismember(redisKey,startUpLog.mid)
    //        jedisClient.close()
    //        !exist
    //      }
    //    )

    //方案二：使用MapPartitions代替filter,减少连接的创建和释放
    //    startUpLogDStream.mapPartitions(
    //      iter=>{
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        val logs: Iterator[StartUpLog] = iter.filter(
    //          startUpLog => {
    //            val redisKey = s"DAU:${startUpLog.logDate}"
    //            !jedisClient.sismember(redisKey, startUpLog.mid)
    //          }
    //        )
    //        jedisClient.close()
    //        logs
    //      }
    //    )

    //方案三：每个批次获取一次连接,访问一次Redis,使用广播变量发送至Executor

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    startUpLogDStream.transform(
      rdd => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val redisKey = s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}"
        val mids: util.Set[String] = jedisClient.smembers(redisKey)
        jedisClient.close()
        val midsBC: Broadcast[util.Set[String]] = sparkContext.broadcast(mids)
        rdd.filter(
          startUpLog => {
            !midsBC.value.contains(startUpLog.mid)
          }
        )
      }
    )

  }

  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            iter.foreach(
              startUpLog => {
                val key: String = s"DAU:${startUpLog.logDate}"
                jedisClient.sadd(key, startUpLog.mid)
              }
            )
            jedisClient.close()
          }
        )
      }
    )
  }

}
