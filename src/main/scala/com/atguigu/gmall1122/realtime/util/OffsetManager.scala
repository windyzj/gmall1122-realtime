package com.atguigu.gmall1122.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
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
    jedis.close()
    import scala.collection.JavaConversions._
    val kafkaOffsetMap: mutable.Map[TopicPartition, Long] = redisOffsetMap.map{case (partitionId,offsetStr)=>( new TopicPartition(topic,partitionId.toInt) ,offsetStr.toLong ) }
    kafkaOffsetMap.toMap
  }


  def saveOffset(groupId:String,topic:String ,offsetRanges:Array[OffsetRange]):Unit={
    if(offsetRanges!=null){
        val jedis: Jedis = RedisUtil.getJedisClient
        val offsetKey="offset:"+groupId+":"+topic
        val offsetMap = new util.HashMap[String,String]()
        //把每个分区的新的偏移量 提取并组合
        var needSaveFlag:Boolean=false

          for ( offsetRange<- offsetRanges ) {
             if(offsetRange.fromOffset<offsetRange.untilOffset){
                  needSaveFlag=true
              }
            //println( "分区:"+offsetRange.partition+"   from "+offsetRange.fromOffset+"->"+offsetRange.untilOffset)
            offsetMap.put(offsetRange.partition.toString,offsetRange.untilOffset.toString)
          }

//          val offsetRange: OffsetRange = offsetRanges(i)
////          if(offsetRange.fromOffset<offsetRange.untilOffset){
////            needSaveFlag=true
////          }
//          //破案：  数组下标i 和 分区数 不对应   保存的时候应该使用 offsetRange.partition 来存分区编号
//          println("i："+i+" 分区:"+offsetRange.partition+"   from "+offsetRange.fromOffset+"->"+offsetRange.untilOffset)
//          offsetMap.put(offsetRange.partition.toString,offsetRange.untilOffset.toString)
//        }
        //把各个分区的新偏移量 写入redis
          if(needSaveFlag){
            jedis.hmset(offsetKey,offsetMap)
          }
         jedis.close()
    }

  }


}
