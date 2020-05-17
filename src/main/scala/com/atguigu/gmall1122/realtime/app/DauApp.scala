package com.atguigu.gmall1122.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.DauInfo
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.hadoop.mapred.Task
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  // 两个问题 1 数组下标i 和 分区数 不对应   保存的时候应该使用 offsetRange.partition 来存分区编号
  // 2   redis的Key的日期（ 发送日志的业务） 和 es存储的索引后缀日期（当前系统日期） 没有对应 取同一天
  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf,Seconds(1))
      val topic="GMALL_START"
      val groupId="GMALL_DAU_CONSUMER"
     //从redis中读取当前最新偏移量
    val startOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic) //d  启动执行一次

    var startInputDstream: InputDStream[ConsumerRecord[String, String]]=null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if(startOffset!=null&&startOffset.size>0){
        startInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,startOffset,groupId)
      //startInputDstream.map(_.value).print(1000)
    }else{
        startInputDstream  = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获得本批次偏移量的移动后的新位置
    var startupOffsetRanges: Array[OffsetRange] =null
    val startupInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream.transform { rdd =>
      //rdd.map(  // ex ).var
      startupOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //d
      rdd
    }






    val startJsonObjDstream: DStream[JSONObject] = startupInputGetOffsetDstream.map { record =>
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
        jedis.expire(dauKey,3600*24*7)

        if(isFirstFlag==1L){
          jsonObjFilteredList+=jsonObj
        }

      }
      jedis.close()
      println("过滤后："+jsonObjFilteredList.size)
      jsonObjFilteredList.toIterator
    }
     //startJsonObjWithDauDstream.print(1000)


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
    //dauInfoDstream println
    //要插入gmall1122_dau_info_2020xxxxxx  索引中
    dauInfoDstream.foreachRDD {rdd=>
        val infoes: Array[DauInfo] = rdd.collect()
      infoes.splitAt(8)//.....d
       rdd.foreachPartition { dauInfoItr =>  //ex
         //观察偏移量移动
         val offsetRange: OffsetRange = startupOffsetRanges(TaskContext.getPartitionId())
         println("偏移量:"+offsetRange.fromOffset+"-->"+offsetRange.untilOffset)
         //写入es
         val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map { dauInfo => (dauInfo.mid, dauInfo) }
         val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
         val indexName = "gmall1122_dau_info_" + dt
         MyEsUtil.saveBulk(dataList, indexName)
       }

      // 偏移量的提交
      OffsetManager.saveOffset(groupId,topic,startupOffsetRanges)//  d   周期性执行
    }

       ssc.start()
       ssc.awaitTermination()

  }



}
