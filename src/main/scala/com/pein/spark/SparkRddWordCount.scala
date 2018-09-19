package com.pein.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkRddWordCount {

  //在windows下运行
  def sparkWindows()={
    System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.7.2")
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf.setAppName("wordCount")
    conf.setMaster("local[1]")
    //conf.setMaster("spark://vm01:7077"),在linux上不用写可以直接运行

    val sc = new SparkContext(conf)

    val file = sc.textFile("hdfs://192.168.226.131:9000/word.txt")


    val words = file.flatMap(_.split(" "))

    val tuple = words.map((_, 1))

    val res = tuple.reduceByKey(_+_)

    val resSort = res.sortBy(_._2, false)

    resSort.saveAsTextFile("hdfs://192.168.226.131:9000/res2")
  }

  //在Linux下运行
  def sparkLinux()={
    val conf = new SparkConf()
    conf.setAppName("wordCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://192.168.226.131:9000/word.txt")
    val words = file.flatMap(_.split(" "))
    val tuple = words.map((_, 1))
    val res = tuple.reduceByKey(_+_)
    val resSort = res.sortBy(_._2, false)
    resSort.saveAsTextFile("hdfs://192.168.226.131:9000/res3")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordCount")
    //conf.setMaster("spark://vm01:7077") 一般来说不在代码中写死，spark命令可以指定
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://vm01:9000/word.txt")
    //val file = sc.textFile(args[0]) 在linux上输入输出可以指定为参数，更灵活
    val words = file.flatMap(_.split(" "))
    val tuple = words.map((_, 1))
    val res = tuple.reduceByKey(_+_)
    val resSort = res.sortBy(_._2, false)
    resSort.saveAsTextFile("hdfs://vm01:9000/res3")
  }

}
