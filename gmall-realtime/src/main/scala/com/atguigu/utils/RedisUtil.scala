package com.atguigu.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool: JedisPool = _

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val JedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
      JedisPoolConfig.setMaxTotal(100)
      JedisPoolConfig.setMaxIdle(20)
      JedisPoolConfig.setMinIdle(20)
      JedisPoolConfig.setBlockWhenExhausted(true)
      JedisPoolConfig.setMaxWaitMillis(500)
      JedisPoolConfig.setTestOnBorrow(true)

      jedisPool= new JedisPool(JedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource
  }
}
