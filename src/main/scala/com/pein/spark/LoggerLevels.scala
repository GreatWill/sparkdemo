package com.pein.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object LoggerLevels extends Logging{
  def setStreamingLogLevels(): Unit ={
    val initial = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!initial){
      logInfo("set level only WARN!")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
