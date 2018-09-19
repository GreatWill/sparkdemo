package com.pein.sparkstreaming

import com.pein.spark.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object StreamingWin {

  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  //hadoop
  System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("tcp").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("./")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.226.131", 9999)

    val maps = lines.flatMap(_.split(" ")).map((_,1))


    val res = maps.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b), Seconds(10), Seconds(10))



    res.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
