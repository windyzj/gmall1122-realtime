package com.atguigu.gmall1122.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf,Seconds(5))
      val topic="GMALL_START"
      val groupId="GMALL_DAU_CONSUMER"
      val startInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
      startInputDstream.map(_.value).print(1000)

    val startJsonObjDstream: DStream[JSONObject] = startInputDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      jSONObject
    }
    //写入去重清单  日活 每天一个清单 key: 每天一个key
    startJsonObjDstream.map{jsonObj=>

      //Redis  写入    type?  set        key?    dau:2020-05-12      value?  mid
      val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))

      val dauKey="dau:"+dateStr
      val jedis = new Jedis("hadoop1",6379)
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      jedis.sadd(dauKey,mid)
      jedis.close()
    }

    startJsonObjDstream.mapPartitions{jsonObjItr=>
     val jedis =  RedisUtil.getJedisClient
      for (jsonObj <- jsonObjItr ) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))

        val dauKey="dau:"+dateStr

        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        jedis.sadd(dauKey,mid)

      }
      jedis.close()
      null
    }

       ssc.start()
       ssc.awaitTermination()

  }



}
