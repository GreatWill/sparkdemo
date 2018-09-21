package com.pein.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.{JsonObject, JsonParser}
import com.pein.spark.LoggerLevels
import com.pein.sparkstreaming.StreamingKafka.sumFun
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import redis.clients.jedis.{Jedis, JedisPool}


object StreamingTest {
  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  //hadoop
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\QQDownloads\\hadoop-2.7.2")

  val jPool = new JedisPool("localhost",6379)

  def save(i: Int):Unit = {
    println("--------"+i+"-----------------")

  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkatest")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("D:\\Files\\tmp\\sparkcheckpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.101.205.20:6667,10.101.205.6:6667,10.101.205.8:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fluentd_to_kudu",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("cjw_topic")
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

    //消息key：id ， value：message
    val lines: DStream[String] = stream.map(record => record.value)

    val jsonStream: DStream[JSONObject] = lines.map(x => JSON.parseObject(x))

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val myJson: DStream[JSONObject] = jsonStream.filter(
      x => x.get("databaseName") == "test_advertisement"
        && x.get("msgType") == "I"
        && x.get("tableName") == "face_id_statistics"
        && String.valueOf(x.get("imageTime")).substring(0,10) == dateFormat.format(new Date())
    )

    val all: DStream[(String, Int)] = myJson.map(x => ("add", 1)).reduceByKey(_+_)

    val value: DStream[Int] = all.map(_._2)


    value.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach(record => save(record))
      }
    }




    ssc.start()
    ssc.awaitTermination()

  }
}
