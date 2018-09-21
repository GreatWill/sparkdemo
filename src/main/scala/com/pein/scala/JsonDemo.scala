package com.pein.scala

import com.google.gson.{Gson, JsonObject, JsonParser}

import scala.util.parsing.json.JSONObject

object JsonDemo {
  def main(args: Array[String]): Unit = {
    val json = "{'name':'tom','age':'13','class':'one'}"
    val jsn = new JsonParser()
    val obj = jsn.parse(json).asInstanceOf[JsonObject]
    println(obj.get("name"))
    println(obj.get("class"))

  }
}
