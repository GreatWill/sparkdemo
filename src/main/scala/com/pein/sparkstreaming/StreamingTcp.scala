package com.pein.sparkstreaming

import com.pein.spark.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTcp {

  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  //hadoop
  System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")



  //newValues 新产生的值， runningCount 历史保存的值
  def selfFun(newValues:Seq[Int], runningCount:Option[Int]): Option[Int] ={
    val newCount = runningCount.getOrElse(0) + newValues.sum
    //Option两个子集，做返回值时，有值用Some(),没有值None()
    //Option是为了让任何值都是一个对象，
    Some(newCount)
  }


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("tcp").setMaster("local[2]")

    //5s内计算一次，但是只是计算着5s内的，没有叠加
    val ssc = new StreamingContext(conf, Seconds(5))

    //累加值是放在内存的，不安全，要求做缓存放在本地，  ./ 当前文件夹
    ssc.checkpoint("./")

    //注意返回数据类型
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.226.131", 9999)

    val maps = lines.flatMap(_.split(" ")).map((_,1))

    //只计算5s内的值
    //val res = maps.reduceByKey(_+_)
    //计算累加值
    val res = maps.updateStateByKey(selfFun _)

    res.print()

    ssc.start()
    ssc.awaitTermination()

    /**
      * 应用  计算一小时流量， 窗口大小1小时，移动时间1小时，计算时间3s，
      *
      *      怎么设置的窗口大小和移动时间
      */

  }
}
