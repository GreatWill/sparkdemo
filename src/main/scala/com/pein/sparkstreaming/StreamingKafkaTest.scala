package com.pein.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{JsonObject, JsonParser}
import com.pein.spark.LoggerLevels
import com.pein.sparkstreaming.StreamingKafka.sumFun
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingKafkaTest {
  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  val sumFun = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap{case(x, y, z)=>Some(y.sum + z.getOrElse(0)).map(v => (x, v))}
  }

  //hadoop
  // System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkatest")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("D:\\Files\\tmp\\sparkcheckpoint")

    //val Array(zkParam, groupId, topics, numThreads) = args 从外部传参
    //val arr = 后面的  然后用arr[0] 麻烦
    //val Array(zkParam, groupId, topics, numThreads) =
    //Array[String]("vm01:2181,vm02:2181,vm03:2181", "g1", "spark_kafka", "2")

    //val topicMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap

    //val lines = KafkaUtils.

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.101.205.20:6667,10.101.205.6:6667,10.101.205.8:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fluentd_to_kudu",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("ad_topic")
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    //消息key：id ， value：message
    val lines: DStream[String] = stream.map(record => record.value)

    val jsn = new JsonParser()

    val jsonStream: DStream[JsonObject] = lines.map(x => jsn.parse(x).asInstanceOf[JsonObject])

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val myJson: DStream[JsonObject] = jsonStream.filter(
      x => x.get("databaseName") == "test_advertisement"
        && x.get("msgType") == "I"
        && x.get("tableName") == "face_id_statistics"
        && dateFormat.format(x.get("imageTime")) == dateFormat.format(new Date())
    )

    val all: DStream[(String, Int)] = myJson.map(x => ("add", 1)).reduceByKey(_+_)

    val value: DStream[Int] = all.map(_._2)

    value.print()

    print("--------hello-----------")

    ssc.start()
    ssc.awaitTermination()

  }
}
