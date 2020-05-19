package com.atguigu.gmall1122.realtime.app.dw

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
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
      if(orderInfoList.size>0){
          val userIdList: List[String] = orderInfoList.map(_.user_id.toString)
          var sql = "select user_id,if_consumed from user_state1122 where user_id in ('" + userIdList.mkString("','") + "')"
          val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

          //避免2重循环  把一个 list转为map
          val userStateMap: Map[String, String] = userStateList.map(userStateJsonObj =>
            //注意返回字段的大小写！！！！！！！！
            (userStateJsonObj.getString("USER_ID"), userStateJsonObj.getString("IF_CONSUMED"))
          ).toMap
          for (orderInfo <- orderInfoList) {
            val userIfConsumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
            if (userIfConsumed != null && userIfConsumed == "1") {
              orderInfo.if_first_order = "0"
            } else {
              orderInfo.if_first_order = "1"
            }
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

//    orderInfoFinalFirstDstream.mapPartitions{
//      //每个分区查一次  12次  100次
//
//    }

    //查询次数过多
//    orderInfoFinalFirstDstream.map{orderInfo=>
//
//
//      val sql="select id ,name , region_id ,area_code  from gmall1122_base_province where id= '"+orderInfo.province_id+"'"
//      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
//      if(provinceJsonObjList!=null&&provinceJsonObjList.size>0){
//        orderInfo.province_name= provinceJsonObjList(0).getString("NAME")
//        orderInfo.province_area_code= provinceJsonObjList(0).getString("AREA_CODE")
//      }
//      orderInfo
//
//    }
// 考虑到查询的整表数据量很小  可以通过一次查询 再通过广播变量进行分发
//问题：如果数据发生变动 无法感知 因为 算子外面的driver操作 只有启动时会执行一次 之后再不执行了
 /*   val sql="select id ,name , region_id ,area_code  from gmall1122_base_province  "   //driver 只执行一次 启动
    val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
    val provinceListBc: Broadcast[List[JSONObject]] = ssc.sparkContext.broadcast(provinceJsonObjList)
    orderInfoFinalFirstDstream.map{orderInfo:OrderInfo=>
          val provinceJsonObjListFromBC: List[JSONObject] = provinceListBc.value   //executor
          //list转map
          val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjListFromBC.map(jsonObj=>(jsonObj.getLongValue("ID"),jsonObj)).toMap
           val provinceJsonObj: JSONObject = provinceJsonObjMap.getOrElse(orderInfo.province_id,null)//从map中寻值
          if(provinceJsonObj!=null){
            orderInfo.province_name=provinceJsonObj.getString("NAME")
            orderInfo.province_area_code=provinceJsonObj.getString("AREA_CODE")
          }
         orderInfo
    }*/

    val orderWithProvinceDstream: DStream[OrderInfo] = orderInfoFinalFirstDstream.transform { rdd =>

      if(rdd.count()>0){
        //driver
        val sql = "select id ,name , region_id ,area_code  from gmall1122_base_province  " //driver  周期性执行
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)

        //list转map
        val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjList.map(jsonObj => (jsonObj.getLongValue("ID"), jsonObj)).toMap
        //广播这个map
        val provinceJsonObjMapBc: Broadcast[Map[Long, JSONObject]] = ssc.sparkContext.broadcast(provinceJsonObjMap)
        val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.mapPartitions { orderInfoItr => //ex
        val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjMapBc.value //接收bc
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          for (orderInfo <- orderInfoList) {
            val provinceJsonObj: JSONObject = provinceJsonObjMap.getOrElse(orderInfo.province_id, null) //从map中寻值
            if (provinceJsonObj != null) {
              orderInfo.province_name = provinceJsonObj.getString("NAME")
              orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
            }
          }
          orderInfoList.toIterator
        }
        orderInfoWithProvinceRDD
      }else{
        rdd
      }

    }

    /////////////// 合并 用户信息////////////////////
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if(orderList.size>0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall1122_user_info where id in ('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
        }
      }
      orderList.toIterator
    }



    orderWithProvinceDstream.cache()

    orderWithProvinceDstream.print(1000)

    orderWithProvinceDstream.foreachRDD { rdd =>
      //写入用户状态
      val userStatRDD: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo =>
        UserState(orderInfo.user_id.toString, orderInfo.if_first_order)
      )
      import org.apache.phoenix.spark._
      userStatRDD.saveToPhoenix("user_state1122",
        Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some("hadoop1,hadoop2,hadoop3:2181"))
    }
         //写入es
    //   println("订单数："+ rdd.count())
    orderWithProvinceDstream.foreachRDD{rdd=>
        rdd.foreachPartition{orderInfoItr=>
          val orderList: List[OrderInfo] = orderInfoItr.toList
          val orderWithKeyList: List[(String, OrderInfo)] = orderList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
          val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
        //  MyEsUtil.saveBulk(orderWithKeyList,"gmall1122_order_info-"+dateStr)

          for (orderInfo <- orderList ) {
            println(orderInfo)
            MyKafkaSink.send("DW_ORDER_INFO",orderInfo.id.toString,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
          }

        }

      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }





    ssc.start()
    ssc.awaitTermination()

  }
}
