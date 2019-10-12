package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.contants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(5))

        val kafkaStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))

        val orderInfoDstrearm: DStream[OrderInfo] = kafkaStream.map {
            case (key, value) => {
                val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
                val timeArray: Array[String] = orderInfo.create_time.split(" ")
                orderInfo.create_date = timeArray(0)
                val timeArr: Array[String] = timeArray(1).split(":")
                orderInfo.create_hour = timeArr(0)
                orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2
                orderInfo
            }
        }
        orderInfoDstrearm.foreachRDD { rdd =>
            val configuration = new Configuration()
            println(rdd.collect().mkString("\n"))
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL0408_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration, Some("hadoop104,hadoop102,hadoop103:2181"))
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
