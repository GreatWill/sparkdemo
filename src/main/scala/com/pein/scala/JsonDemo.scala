package com.pein.scala

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.google.gson.{Gson, JsonObject, JsonParser}

import scala.util.parsing.json.JSONObject

object JsonDemo {
  def main(args: Array[String]): Unit = {
    val str = "{\"advertiserId\":1,\"age\":29,\"databaseName\":\"test_advertisement\",\"equno\":\"DJ38a28cd9153a\",\"faceFrame\":\"/upload/report/2018-09-21/DJ38a28cd9153a/b1e14c84-efee-424d-b506-f7c5a349cfee.jpg\",\"id\":2018092100000081,\"imageTime\":\"2018-09-21 12:31:26\",\"msgType\":\"I\",\"msgid\":\"b1e14c84-efee-424d-b506-f7c5a349cfee\",\"personIds\":\"[]\",\"repeats\":\"[]\",\"resideTime\":1,\"sex\":\"女\",\"shopId\":\"x20180919114730030\",\"star\":2,\"tableName\":\"face_id_statistics\",\"type\":0}"
    val json = JSON.parseObject(str)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")


    val str22: String = String.valueOf(json.get("imageTime")).substring(0,10)

    println(str22)

    val str33 = dateFormat.format(new Date())

    print(str33)

    println(str22 == str33)





  }
}
