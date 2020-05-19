package com.atguigu.gmall1122.realtime.app.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1122.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.atguigu.gmall1122.realtime.bean.dim.SkuInfo
import com.atguigu.gmall1122.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWide1122App {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dws_order_wide_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicOrderInfo = "DW_ORDER_INFO";
    val topicOrderDetail = "DW_ORDER_DETAIL";
    val groupId = "dws_order_wide_group"


    /////////////////////  偏移量处理///////////////////////////
    var offsetOrderInfo: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topicOrderInfo)
    var offsetOrderDetail: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topicOrderDetail)
    offsetOrderInfo=null
    offsetOrderDetail=null
    var inputOrderInfoDstream: InputDStream[ConsumerRecord[String, String]] = null
    var inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    //加载orderInfo流
    if (offsetOrderInfo != null && offsetOrderInfo.size > 0) {
      inputOrderInfoDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, offsetOrderInfo, groupId)
    } else {
      inputOrderInfoDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupId)
    }
    //加载orderDetail流
    if (offsetOrderInfo != null && offsetOrderInfo.size > 0) {
      inputOrderDetailDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, offsetOrderDetail, groupId)
    } else {
      inputOrderDetailDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupId)
    }


    //取得偏移量步长
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val inputOrderInfoGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputOrderInfoDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val inputOrderDetailGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputOrderDetailDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    val orderInfoDstream: DStream[(Long,OrderInfo)] = inputOrderInfoGetOffsetDstream.map { record =>
      val orderJsonstr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(orderJsonstr, classOf[OrderInfo])
      ( orderInfo.id,orderInfo)
    }

    val orderDetailDstream: DStream[(Long,OrderDetail)] = inputOrderDetailGetOffsetDstream.map { record =>
      val orderDetailJsonstr: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonstr, classOf[OrderDetail])
      ( orderDetail.order_id,orderDetail  )
    }
    //窗口开小了 数据延迟大的话 还是会出现丢失 //窗口开大了 会造成大量冗余数据
    val orderInfoWindowDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.window(Seconds(15),Seconds(5))
    val orderDetailWindowDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.window(Seconds(15),Seconds(5))

    // 会不会出现shuffle  // 让相同订单的明细保持在一个分区下-> 写入kafka时 ，选用order_id 作为key
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowDstream.join(orderDetailWindowDstream)

    val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinedDstream.map{case (orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}

    //去重
    val orderDetailWideFilteredDstream: DStream[OrderDetailWide] = orderDetailWideDstream.mapPartitions { orderWideItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      val orderWideFilteredList: ListBuffer[OrderDetailWide] = ListBuffer[OrderDetailWide]()
      for (orderWide <- orderWideList) {
        // redis    type  sadd   key  order_wide:order_id:[order_id]  value  [order_detail_id]  expire 600
        val orderWideKey = "order_wide:order_id:" + orderWide.order_id
        val isNew: lang.Long = jedis.sadd(orderWideKey, orderWide.order_detail_id.toString)
        jedis.expire(orderWideKey, 600)
        if (isNew == 1L) {
          orderWideFilteredList += orderWide
        }
      }
      jedis.close()
      orderWideFilteredList.toIterator
    }


    //  orderWide. final_total_amount 实付总金额
    //             origin_total_amount  应付总金额 =  sum( sku_price* sku_num )  单价* 个数
    //             sku_price 商品单价  sku_num 商品购买个数
    // 目标 ： final_detail_amount  明细分摊实付金额       明细分摊实付金额 / 实付总金额 = (单价* 个数)/ 应付总金额
               //但是还要考虑    必须保证 sum(明细分摊实付金额) = 实付总金额

    orderDetailWideFilteredDstream.mapPartitions { orderWideItr =>
      // jedis
      // 首先 从 redis中取得该笔名的已经存入（计算）的兄弟明细数据
      //  把兄弟明细的应收金额+自己的应收金额 得到一个汇总值
      //   把兄弟明细中的实收分摊汇总值 求出

      //比较  主订单的应收总值是否= 明细的应收汇总值
      //如果不等于 （非最后一笔）
      // 用乘除占比 求得 该明细分摊金额
      //如果等于
      // 用减法 总实付-兄弟明细的分摊汇总

      //把当前明细的计算结果保存到redis 中
      //保存到某个list里  返回list的迭代器
      null
    }














      null
    }







    orderDetailWideFilteredDstream.print(1000)


    ssc.start()
    ssc.awaitTermination()



  }

}
