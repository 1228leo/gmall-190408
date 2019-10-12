package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.StartUpLog
import com.atguigu.contants.GmallConstants
import com.atguigu.handle.DAUHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
    // upload{"area":"shandong","uid":"192","os":"ios","ch":"appstore","appid":"gmall2019","mid":"mid_55","type":"startup","vs":"1.2.0"}
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTime")

        val ssc = new StreamingContext(conf, Seconds(5))

        val kafkaStreaming: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP))
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val startUpLogDStream: DStream[StartUpLog] = kafkaStreaming.map {
            case (k, v) => {
                val startLog: StartUpLog = JSON.parseObject(v, classOf[StartUpLog])
                val ts = startLog.ts
                val dateHour: String = sdf.format(new Date(ts))
                val dateHourArray: Array[String] = dateHour.split(" ")
                startLog.logDate = dateHourArray(0)
                startLog.logHour = dateHourArray(1)
                startLog
            }
        }
        //Redis去重（不用批次）
        val filterStartUpLogDStream: DStream[StartUpLog] = DAUHandle.filterDataByRedis(ssc, startUpLogDStream)

        //相同批次去重
        val distinctStartUpLogDStream: DStream[StartUpLog] = filterStartUpLogDStream.map(log => (log.mid, log)).groupByKey().flatMap {
            case (k, v) => {
                v.toList.take(1)
            }
        }
        // 将数据保存到Redis
        DAUHandle.saveUserToRedis(distinctStartUpLogDStream)

        distinctStartUpLogDStream.foreachRDD{rdd=>
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL190408_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
