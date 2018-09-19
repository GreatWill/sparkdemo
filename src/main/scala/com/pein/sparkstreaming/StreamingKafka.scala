package com.pein.sparkstreaming

import com.pein.spark.LoggerLevels
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingKafka {

  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  //hadoop
  System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")

  val sumFun = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap{case(x, y, z)=>Some(y.sum + z.getOrElse(0)).map(v => (x, v))}
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./")

    //val Array(zkParam, groupId, topics, numThreads) = args 从外部传参
    //val arr = 后面的  然后用arr[0] 麻烦
    //val Array(zkParam, groupId, topics, numThreads) =
      //Array[String]("vm01:2181,vm02:2181,vm03:2181", "g1", "spark_kafka", "2")

    //val topicMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap

    //val lines = KafkaUtils.

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "vm01:9092,vm02:9092,vm02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //消息key：id ， value：message
    val lines: DStream[String] = stream.map(record => record.value)

    val res = lines.flatMap(_.split(" ")).map((_, 1))

    val result = res.updateStateByKey(sumFun, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
