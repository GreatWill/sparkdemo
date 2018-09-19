package com.pein.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddPvAndUv {

  //设置日志打印级别
  LoggerLevels.setStreamingLogLevels()

  //hadoop
  System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")

  def getUv(file:RDD[String])={
    //分割就是对每一行的分割，返回数组， 然后每个数组取第一个即ip，统计
    val uvArray: RDD[Array[String]] = file.map(_.split(","))
    //注意去重
    val uvMap: RDD[(String, Int)] = uvArray.map(x=>x(0)).distinct().map(x=>("uv",1))
    val res: RDD[(String, Int)] = uvMap.reduceByKey(_+_)
    res.foreach(println)

  }

  def getPv(file:RDD[String])={
    //一行数据就是一个点击
    val words = file.map( x => ("pv", 1))
    val res = words.reduceByKey(_+_)
    res.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pv").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //文件读出来就是一行一行数据,注意是一个RDD[String]类型,所有的操作都是返回RDD
    val file = sc.textFile("D:\\Documents\\tmp\\log.txt")
    getPv(file)
    getUv(file)
  }
}
