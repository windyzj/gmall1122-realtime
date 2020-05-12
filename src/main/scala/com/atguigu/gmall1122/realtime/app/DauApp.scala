package com.atguigu.gmall1122.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.DauInfo
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf,Seconds(5))
      val topic="GMALL_START"
      val groupId="GMALL_DAU_CONSUMER"

      val startOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic)

      val startInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc,startOffset,groupId)
      //startInputDstream.map(_.value).print(1000)



    val startJsonObjDstream: DStream[JSONObject] = startInputDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      jSONObject
    }
    //写入去重清单  日活 每天一个清单 key: 每天一个key   //不够优化，连接次数较多
//    startJsonObjDstream.map{jsonObj=>
//
//      //Redis  写入    type?  set        key?    dau:2020-05-12      value?  mid
//      val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))
//
//      val dauKey="dau:"+dateStr
//      val jedis = new Jedis("hadoop1",6379)
//      val mid: String = jsonObj.getJSONObject("common").getString("mid")
//      jedis.sadd(dauKey,mid)
//      jedis.close()
//    }


    val startJsonObjWithDauDstream: DStream[JSONObject] = startJsonObjDstream.mapPartitions { jsonObjItr =>
      val jedis = RedisUtil.getJedisClient
      val jsonObjList: List[JSONObject] = jsonObjItr.toList
      println("过滤前："+jsonObjList.size)
      val jsonObjFilteredList = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjList) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))
        val dauKey = "dau:" + dateStr
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        if(isFirstFlag==1L){
          jsonObjFilteredList+=jsonObj
        }

      }
      jedis.close()
      println("过滤后："+jsonObjFilteredList.size)
      jsonObjFilteredList.toIterator
    }
    // startJsonObjWithDauDstream.print(1000)


    // 变换结构
    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDstream.map { jsonObj =>
      val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(jsonObj.getLong("ts")))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)

      DauInfo(commonJsonObj.getString("mid"),
        commonJsonObj.getString("uid"),
        commonJsonObj.getString("ar"),
        commonJsonObj.getString("ch"),
        commonJsonObj.getString("vc"),
        dt, hr, mi, jsonObj.getLong("ts")
      )

    }
    //要插入gmall1122_dau_info_2020xxxxxx  索引中
    dauInfoDstream.foreachRDD {rdd=>
       rdd.foreachPartition { dauInfoItr =>
         val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map { dauInfo => (dauInfo.mid, dauInfo) }
         val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
         val indexName = "gmall1122_dau_info_" + dt
         MyEsUtil.saveBulk(dataList, indexName)
       }
    }
       ssc.start()
       ssc.awaitTermination()

  }



}
