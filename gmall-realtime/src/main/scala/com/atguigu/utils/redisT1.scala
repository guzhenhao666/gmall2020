//package com.atguigu.utils
//
//import java.util.Properties
//
//import redis.clients.jedis.{JedisPool, JedisPoolConfig}
//
//object redisT1 {
//  var jedisPool :JedisPool=_
//
//  def getJedisClient={
//    if (jedisPool ==null){
//      println("open")
//      val config: Properties = PropertiesUtil.load("config.properties")
//      val host: String = config.getProperty("redis.host")
//      val port: String = config.getProperty("redis.port")
//
//      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
//
//      jedisPoolConfig.setMaxTotal(100)
//      jedisPoolConfig.setMaxIdle(20)
//      jedisPoolConfig.setMinIdle(20)
//      jedisPoolConfig.setBlockWhenExhausted(true)
//      jedisPoolConfig.setMaxWaitMillis(500)
//      jedisPoolConfig.setTestOnBorrow(true)
//
//      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
//    }
//    jedisPool.getResource
//  }
//
//}
