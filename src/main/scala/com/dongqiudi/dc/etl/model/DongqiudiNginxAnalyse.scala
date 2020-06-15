package com.dongqiudi.dc.etl.model

import java.util

import com.dongqiudi.dc.util.{IPExt, LogConvertUtil}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * 推送日志模型
  * Created by matrix on 17/11/16.
  */
object DongqiudiNginxAnalyse extends Serializable with Logging {

  val struct = StructType(Array(
    StructField("uuid", StringType),
    StructField("lang", StringType),
    StructField("platform", StringType),
    StructField("version", StringType),
    StructField("country", StringType),

    StructField("province", StringType),
    StructField("log_timestamp", StringType),
    StructField("server_addr", StringType),
    StructField("remote_addr", StringType),
    StructField("xff", StringType),

    StructField("scheme", StringType),
    StructField("host", StringType),
    StructField("method", StringType),
    StructField("uri", StringType),
    StructField("url", StringType),

    StructField("protocol", StringType),
    StructField("status", StringType),
    StructField("size", StringType),
    StructField("request_time", StringType),
    StructField("upstream_time", StringType),

    StructField("upstream_host", StringType),
    StructField("referer", StringType),
    StructField("agent", StringType),
    StructField("token", StringType),
    StructField("body", MapType(StringType, StringType)),

    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class DongqiudiNginxAnalyseLogModel {
    @JsonProperty("@timestamp") @BeanProperty var timestamp = ""
    @JsonProperty("server_addr") @BeanProperty var serverAddr = ""
    @JsonProperty("remote_addr") @BeanProperty var remoteAddr = ""
    @JsonProperty("scheme") @BeanProperty var scheme = ""
    @JsonProperty("host") @BeanProperty var host = ""
    @JsonProperty("method") @BeanProperty var method = ""
    @JsonProperty("uri") @BeanProperty var uri = ""
    @JsonProperty("url") @BeanProperty var url = ""
    @JsonProperty("protocol") @BeanProperty var protocol = ""
    @JsonProperty("status") @BeanProperty var status = ""
    @JsonProperty("size") @BeanProperty var size = ""
    @JsonProperty("request_time") @BeanProperty var requestTime = ""
    @JsonProperty("upstream_time") @BeanProperty var upstreamTime = ""
    @JsonProperty("upstream_host") @BeanProperty var upstreamHost = ""
    @JsonProperty("referer") @BeanProperty var referer = ""
    @JsonProperty("agent") @BeanProperty var agent = ""
    @JsonProperty("xff") @BeanProperty var xff = ""
    @JsonProperty("uuid") @BeanProperty var uuid = ""
    @JsonProperty("authorization") @BeanProperty var authorization = ""
    @JsonProperty("lang") @BeanProperty var lang = ""
    @JsonProperty("body") @BeanProperty var body: util.HashMap[String, String] = new util.HashMap[String, String]()
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      var newLine = line

      // 替换 logstash 引入的特殊字符
      newLine = newLine.replaceAll("\\\\x22", "\"")
      newLine = newLine.replaceAll("\\\\x5C", "")

      // 替换未能正确格式化为 json 的数据
      newLine = newLine.replaceAll("\"body\":-", "\"body\":{}")
      newLine = newLine.replaceAll("\"body\":\"-\"", "\"body\":{}")

      if (newLine.contains("\"body\":platform")) {
        val idx = newLine.indexOf("\"body\":platform")
        newLine = newLine.substring(0, idx+7) + "{\"platform\":\"" + newLine.substring(idx+16, newLine.length-2) + "\"}}"
      }

      if (newLine.contains("\"body\":\"platform")) {
        val idx = newLine.indexOf("\"body\":\"platform")
        newLine = newLine.substring(0, idx+7) + "{\"platform\":\"" + newLine.substring(idx+17, newLine.length-3) + "\"}}"
      }

      if (newLine.contains("\"message\":\"{")) {
        val start_idx = newLine.indexOf("\"message\":\"{") + 11
        val end_idx = newLine.indexOf("\",\"type\":\"in-analyse\",\"tags\":[\"_jsonparsefailure\"]}")
        newLine = newLine.substring(start_idx, end_idx).replace("\\\"", "\"")
      }

      newLine = newLine.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[DongqiudiNginxAnalyseLogModel])

      // timestamp
      val timeMSG = LogConvertUtil.getTimeMSG(logModel.getTimestamp)
      val timestamp = timeMSG(0)
      val day = timeMSG(1)
      val hour = timeMSG(2)

      // platform & version
      var platform = "-"
      var version = "-"
      var message = LogConvertUtil.uaMap.get(logModel.getAgent)
      if (message == null) {
        platform = LogConvertUtil.getPlatform(logModel.getAgent)
        version = LogConvertUtil.getVersion(logModel.getAgent)
        LogConvertUtil.uaMap.put(logModel.getAgent, platform + "\t" + version)
      } else {
        val array = message.split("\t")
        platform = array(0)
        version = array(1)
      }

      // country & province
      var country = "-"
      var province = "-"
      var ip = if (logModel.getXff.equals("-")) logModel.getRemoteAddr else logModel.getXff.split(",")(0)
      if (!ip.contains(".")) {
        ip = logModel.getRemoteAddr
      }

      message = LogConvertUtil.ipMap.get(ip)
      if (message == null) {
        // GeoIP 使用方法
        /*val ipAddress = InetAddress.getByName(ip)
        val cityResponse = LogConvertUtil.ipReader.city(ipAddress)
        country = cityResponse.getCountry.getNames.get("zh-CN")
        province = cityResponse.getMostSpecificSubdivision.getNames.get("zh-CN")*/

        // IPIP.net 使用方法
        val array = IPExt.getInstance().find(ip)
        // val array = Array()
        if (array.length > 1) {
          country = array(0)
          province = array(1)
        }
        if (province == null) {
          province = country
        }
        message = country + "\t" + province
        LogConvertUtil.ipMap.put(ip, message)
      }
      val array = message.split("\t", -1)
      if (array.length > 1) {
        country = array(0)
        province = array(1)
      }
      if (LogConvertUtil.special.contains(country)) {
        country = province
        if (array.length > 2) {
          province = array(2)
        }
      }

      // token
      if (logModel.getAuthorization.length == 0) {
        logModel.setAuthorization("-")
      }

      Row(
        logModel.getUuid,
        logModel.getLang,
        platform,
        version,
        country,

        province,
        timestamp,
        logModel.getServerAddr,
        logModel.getRemoteAddr,
        logModel.getXff,

        logModel.getScheme,
        logModel.getHost,
        logModel.getMethod,
        logModel.getUri,
        logModel.getUrl,

        logModel.getProtocol,
        logModel.getStatus,
        logModel.getSize,
        logModel.getRequestTime,
        logModel.getUpstreamTime,

        logModel.getUpstreamHost,
        logModel.getReferer,
        logModel.getAgent,
        logModel.getAuthorization,
        logModel.getBody,

        day,
        hour
      )
    } catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row(0)
    }
  }
}
