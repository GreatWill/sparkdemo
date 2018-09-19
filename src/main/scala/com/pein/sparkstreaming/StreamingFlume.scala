package com.pein.sparkstreaming

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从flume拉取数据
  */
object StreamingFlume {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flume").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./")
    //ssc.checkpoint("hdfs://")
    val adr = Seq(new InetSocketAddress("192.168.226.131", 9999))
    val lines = FlumeUtils.createPollingStream(ssc, adr, StorageLevel.MEMORY_AND_DISK)
    //获取body，分割，累加

  }

}
