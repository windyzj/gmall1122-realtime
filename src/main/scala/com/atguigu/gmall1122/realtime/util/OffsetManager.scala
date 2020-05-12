package com.atguigu.gmall1122.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManager {



  //把redis中的偏移量读取出来 ，并转换成Kafka需要偏移量格式
  def getOffset(groupId:String,topic:String ):Map[TopicPartition,Long]={
    val jedis: Jedis = RedisUtil.getJedisClient
    //Redis   type?  hash       key?   "offset:[groupid]:[topic]       field ?  partition  value?   offset
     val offsetKey="offset:"+groupId+":"+topic
    //通过一个key查询hash的所有值
    val redisOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    import scala.collection.JavaConversions._
    val kafkaOffsetMap: mutable.Map[TopicPartition, Long] = redisOffsetMap.map{case (partitionId,offsetStr)=>( new TopicPartition(topic,partitionId.toInt) ,offsetStr.toLong ) }
    kafkaOffsetMap.toMap
  }



}
