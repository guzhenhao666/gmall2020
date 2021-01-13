package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)

    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(
      record => {
        val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        val create_time: String = info.create_time
        val dateTime: Array[String] = create_time.split(" ")
        info.create_date = dateTime(0)
        info.create_hour = dateTime(1).split(":")(0)
        val consignee_tel: String = info.consignee_tel
        val tuple: (String, String) = consignee_tel.splitAt(4)
        info.consignee_tel = s"${tuple._1}*******"
        (info.id, info)
      }
    )
    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    })

    val fullJoinResult: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
      orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)


    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinResult.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //创建集合用于存放JOIN上的数据
      val details = new ListBuffer[SaleDetail]()

      //样例类转换为JSON字符串的格式化对象
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //遍历迭代器使用连接
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //RedisKey
        val infoRedisKey = s"ORDERINFO:$orderId" //String
        val detailRedisKey = s"ORDERDETAIL:$orderId" //Set

        //a.info数据不为空
        if (infoOpt.isDefined) {

          //提取info数据
          val orderInfo: OrderInfo = infoOpt.get

          //a.1 detail数据不为空
          if (detailOpt.isDefined) {
            //提取detail数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合info和detail数据并放入集合
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            details += saleDetail
          }

          //a.2 将info数据写入缓存
          //val orderInfoStr: String = JSON.toJSONString(orderInfo)  //报错
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)
          jedisClient.expire(infoRedisKey, 100)

          //a.3 查询detail缓存数据
          val detailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          detailSet.asScala.foreach(detailJsonStr => {
            val orderDetail: OrderDetail = JSON.parseObject(detailJsonStr, classOf[OrderDetail])
            details += new SaleDetail(orderInfo, orderDetail)
          })
        }

        //b.info数据为空
        else {

          //提取Detail数据
          val orderDetail: OrderDetail = detailOpt.get

          //b.1 查询info缓存数据,存在
          if (jedisClient.exists(infoRedisKey)) {

            //查出数据,转换为样例类对象,结合写入集合
            val infoJsonStr: String = jedisClient.get(infoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(infoJsonStr, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)

          }
          //b.2 查询info缓存数据,不存在,将detail数据写入缓存
          else {
            val detailStr: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }
        }
      }

      //归还连接
      jedisClient.close()

      //返回结果
      details.iterator
    })


    //6.查询Redis中的用户信息,补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //遍历迭代器补全用户信息
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        //根据UserID查询用户信息
        val userInfoStr: String = jedisClient.get(s"UserInfo:${saleDetail.user_id}")
        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
        //补全用户信息
        saleDetail.mergeUserInfo(userInfo)
        //返回
        saleDetail
      })

      //归还连接
      jedisClient.close()

      //返回数据
      details
    })


    //7.将数据写入ES
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //索引名
        val indexName = s"${GmallConstant.ES_SALE_DETAIL_INDEX_PRE}_${sdf.format(new Date(System.currentTimeMillis()))}"

        //补充DocID
        val list: List[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id, saleDetail)).toList

        //执行批量数据写入操作
        MyEsUtil.insertBulk(indexName, list)

      })

    })




      //启动任务
      ssc.start()
      ssc.awaitTermination()

    }
  }
