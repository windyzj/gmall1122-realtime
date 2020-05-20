package com.atguigu.gmall1122.realtime.app.ads

import java.lang.Math

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1122.realtime.bean.OrderDetailWide
import com.atguigu.gmall1122.realtime.bean.dim.BaseCategory3
import com.atguigu.gmall1122.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import java.lang.Math
object TrademarkAmountSumApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ads_trademark_sum_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_DETAIL_WIDE";
    val groupId = "ads_trademark_sum_group"


    /////////////////////  偏移量处理///////////////////////////
    ////  改成 //mysql
    val offset: Map[TopicPartition, Long] = OffsetManagerM.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //转换结构
    val orderDetailWideDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonStr, classOf[OrderDetailWide])
      orderDetailWide
    }

    val orderWideWithKeyDstream: DStream[(String, Double)] = orderDetailWideDstream.map { orderDetailWide =>
      (orderDetailWide.tm_id + ":" + orderDetailWide.tm_name, orderDetailWide.final_detail_amount)
    }
    val orderWideSumDstream: DStream[(String, Double)] = orderWideWithKeyDstream.reduceByKey ((amount1,amount2)=>
       java.lang.Math.round((amount1+amount2)/100)*100
     )

    orderWideSumDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()

  }




}
