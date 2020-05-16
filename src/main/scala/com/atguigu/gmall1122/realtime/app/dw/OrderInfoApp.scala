package com.atguigu.gmall1122.realtime.app.dw

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall1122.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderInfoApp {


  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dw_order_info_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_T_ORDER_INFO";
    val groupId = "base_order_info_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
      //startInputDstream.map(_.value).print(1000)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    /////////////////////  业务处理///////////////////////////

   //基本转换 补充日期字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])

      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date=datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour=timeArr(0)
      orderInfo
    }

    val orderInfoWithfirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      val userIdList: List[String] = orderInfoList.map(_.user_id.toString)
      var sql = "select user_id,if_consumed from user_state1122 where user_id in (" + userIdList.mkString(",") + ")"
      val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

      //避免2重循环  把一个 list转为map
      val userStateMap: Map[String, String] = userStateList.map(userStateJsonObj =>
        (userStateJsonObj.getString("user_id"), userStateJsonObj.getString("if_consumed"))
      ).toMap
      for (orderInfo <- orderInfoList) {
        val userIfConsumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
        if (userIfConsumed != null && userIfConsumed == "1") {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
      }
      orderInfoList.toIterator
    }

    // 解决 同一批次同一用户多次下单  如果是首次消费 ，多笔订单都会被认为是首单
    val orderInfoWithUidDstream: DStream[(Long, OrderInfo)] = orderInfoWithfirstDstream.map(orderInfo=>(orderInfo.user_id,orderInfo))
    val orderInfoGroupbyUidDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithUidDstream.groupByKey()
    val orderInfoFinalFirstDstream: DStream[OrderInfo] = orderInfoGroupbyUidDstream.flatMap { case (userId, orderInfoItr) =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList(0).if_first_order == "1" && orderInfoList.size > 1) { //有首单标志的用户订单集合才进行处理
        //把本批次用的订单进行排序
        val orderInfoSortedList: List[OrderInfo] = orderInfoList.sortWith { (orderInfo1, orderInfo2) => (orderInfo1.create_time < orderInfo2.create_time) }
        for (i <- 1 to orderInfoSortedList.size - 1) {
          orderInfoSortedList(i).if_first_order = "0" //除了第一笔全部置为0 （非首单)
        }
        orderInfoSortedList.toIterator
      } else {
        orderInfoList.toIterator
      }

    }


    orderInfoFinalFirstDstream.foreachRDD{rdd=>
          val userStatRDD:RDD[UserState]  = rdd.filter(_.if_first_order=="1").map(orderInfo=>
            UserState(orderInfo.user_id.toString,orderInfo.if_first_order)
          )
         import org.apache.phoenix.spark._
         userStatRDD.saveToPhoenix("user_state1122",
           Seq("USER_ID","IF_CONSUMED"),
           new Configuration,
           Some("hadoop1,hadoop2,hadoop3:2181"))

    }




   /* dbJsonObjDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonObjItr =>

        for (jsonObj <- jsonObjItr) {
          val dataObj: JSONObject = jsonObj.getJSONObject("data")
          val tableName = jsonObj.getString("table")
          val id = dataObj.getString("id")
          val topic = "ODS_T_" + tableName.toUpperCase
          MyKafkaSink.send(topic, id, dataObj.toJSONString)
        }
      }
      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }*/
    ssc.start()
    ssc.awaitTermination()

  }
}
