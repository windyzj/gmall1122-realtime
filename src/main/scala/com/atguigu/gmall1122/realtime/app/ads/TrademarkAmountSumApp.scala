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
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs
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
    //保存数据字段：时间(业务时间也行)，维度， 度量    stat_time ,tm_id,tm_name,amount,(sku_num)(order_count)
    //保存偏移量


    orderWideSumDstream.foreachRDD{rdd:RDD[(String, Double)]=>
      //把各个executor中各个分区的数据收集到driver中的一个数组
       val orderWideArr: Array[(String, Double)] = rdd.collect()
      // scalikejdbc
      if(orderWideArr!=null&&orderWideArr.size>0){
          DBs.setup()
          DB.localTx{implicit session=>  //事务启动
            // 偏移量保存完毕
            for (offset <- offsetRanges ) {
              println(offset.partition+"::"+offset.untilOffset)
              SQL("REPLACE INTO  `offset_1122`(group_id,topic, partition_id, topic_offset)  VALUES( ?,?,?,?)  ").bind(groupId,topic,offset.partition,offset.untilOffset).update().apply()
            }
          //  throw new RuntimeException("强行异常测试！！！！")
            //整个集合保存完毕
            for ((tm,amount)  <- orderWideArr ) {
              val statTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:dd").format(new Date)
              val tmArr: Array[String] = tm.split(":")
              val tm_id=tmArr(0)
              val tm_name=tmArr(1)
              SQL("insert into trademark_amount_sum_stat values(?,?,?,?)").bind(statTime,tm_id,tm_name,amount).update().apply()
            }

          }//事务结束

        }
    }


    orderWideSumDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()

  }




}
