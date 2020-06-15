package com.dongqiudi.dc.util

import java.util.Locale
import java.util.concurrent.ConcurrentSkipListMap
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}
import org.apache.spark.Logging

/**
  * 日志转换工具类
  * Created by matrix on 17/8/29.
  */
object LogConvertUtil extends Serializable with Logging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val androidPattern: Pattern = Pattern.compile(".*android.*", Pattern.CASE_INSENSITIVE)
  val iPhonePattern: Pattern = Pattern.compile(".*iphone.*", Pattern.CASE_INSENSITIVE)
  val iPadPattern: Pattern = Pattern.compile(".*ipad.*", Pattern.CASE_INSENSITIVE)
  val iPodPattern: Pattern = Pattern.compile(".*ipod.*", Pattern.CASE_INSENSITIVE)
  val iosPattern: Pattern = Pattern.compile(".*ios.*", Pattern.CASE_INSENSITIVE)
  val wpPattern: Pattern = Pattern.compile("NativeHost", Pattern.CASE_INSENSITIVE)

  val versionPattern: Pattern = Pattern.compile(".*(newsapp|appfootball)/([\\d.]+).*", Pattern.CASE_INSENSITIVE)

  val timeFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", Locale.ENGLISH)

  // 存储已完成转换的时间
  val timeMap = new ConcurrentSkipListMap[String, String]

  // 存储已解析的 UA
  val uaMap = new ConcurrentSkipListMap[String, String]

  // 使用 GeoIP2 解析国家省市信息
  // val database = new File("/data/plugins/data/GeoLite2-City.mmdb")
  // val database = new File("/Users/matrix/Downloads/GeoLite2-City_20170801/GeoLite2-City.mmdb")
  // val ipReader: DatabaseReader = new DatabaseReader.Builder(database).build()

  // 存储已解析的国家城市信息
  val ipMap = new ConcurrentSkipListMap[String, String]
  val special = Set("香港", "澳门", "台湾")

  def getPlatform(userAgent: String): String = {
    var platform = "-"
    if (androidPattern.matcher(userAgent).find()) {
      platform = "android"
    } else if (iPhonePattern.matcher(userAgent).find()) {
      platform = "iphone"
    } else if (iPadPattern.matcher(userAgent).find()) {
      platform = "ipad"
    } else if (iPodPattern.matcher(userAgent).find()) {
      platform = "ipod"
    } else if (iosPattern.matcher(userAgent).find()) {
      platform = "ios"
    } else if (wpPattern.matcher(userAgent).find()) {
      platform = "wp"
    }
    platform
  }

  def getVersion(userAgent: String): String = {
    var version = "-"
    val m = versionPattern.matcher(userAgent)
    if (m.find()) {
      version = m.group(2)
    }
    version
  }

  def getTimeMSG(time: String): Array[String] = {
    if (!timeMap.containsKey(time)) {
      var timeStamp = DateUtils.parseDate(time, Locale.ENGLISH, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ", "yyyy-MM-dd'T'HH:mm:ssZZ", "yyyy-MM-dd'T'HH:mm:ss.S'Z'", "dd/MMM/yyyy:HH:mm:ss Z").getTime
      if (time.endsWith("Z")) {
        timeStamp += 28800000
      }
      val timeStr = timeFormat.format(timeStamp)
      val day = LogConvertUtil.getDay(timeStr)
      val hour = LogConvertUtil.getHour(timeStr)
      timeMap.put(time, "" + timeStamp + "#" + day + "#" + hour)
    }
    timeMap.get(time).split("#")
  }

  /**
    * 获取日期
    *
    * @param time yyyy-MM-dd'T'HH:mm:ssZZ
    * @return yyyyMMdd
    */
  def getDay(time: String): String = {
    time.substring(0, 10).replaceAll("-", "")
  }

  /**
    * 获取小时
    *
    * @param time yyyy-MM-dd'T'HH:mm:ssZZ
    * @return HH
    */
  def getHour(time: String): String = {
    time.substring(11, 13)
  }

}
