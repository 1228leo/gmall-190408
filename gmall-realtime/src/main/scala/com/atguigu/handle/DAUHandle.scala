package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DAUHandle {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    def filterDataByRedis(ssc: StreamingContext, startUpLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
        startUpLogDStream.transform(rdd => {

            val jedis: Jedis = RedisUtil.getJedisClient
            val date = sdf.format(new Date(System.currentTimeMillis()))
            val mids: util.Set[String] = jedis.smembers(s"dau:${date}")
            val midsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
            jedis.close()
            val result: RDD[StartUpLog] = rdd.filter(log => {
                val midsBCValue: util.Set[String] = midsBC.value
                !midsBCValue.contains(log.mid)
            })
            result
        })
    }

    def saveUserToRedis(startUpLogDStream: DStream[StartUpLog]) = {
        startUpLogDStream.foreachRDD {
            log => {
                log.foreachPartition(items => {
                    val jedis: Jedis = RedisUtil.getJedisClient
                    items.foreach(startLog => {
                        val redisKey = s"dau:${startLog.logDate}"
                        jedis.sadd(redisKey, startLog.mid)
                    })
                    jedis.close()
                })
            }
        }
    }

}
