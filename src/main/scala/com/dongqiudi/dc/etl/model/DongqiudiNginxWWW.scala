package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.etl.model.DongqiudiNginxAPI.{DongqiudiNginxAPILogModel, log2json, logError}
import com.dongqiudi.dc.util.{IPExt, LogConvertUtil}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  *
  * Created by matrix on 18/2/28.
  */
object DongqiudiNginxWWW extends Serializable with Logging {
  val struct = StructType(Array(
    StructField("uuid", StringType),
    StructField("lang", StringType),
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
    StructField("dqduid", StringType),
    StructField("sign", StringType),

    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class DongqiudiNginxWWWLogModel {
    @JsonProperty("@timestamp") @BeanProperty var timestamp = ""
    @JsonProperty("@version") @BeanProperty var version = ""
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
    @JsonProperty("sign") @BeanProperty var sign = ""
    @JsonProperty("dqduid") @BeanProperty var dqdUID = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      var newLine = line
      if (newLine.contains("_jsonparsefailure")) {
        val start_idx = newLine.indexOf("\"message\":\"{") + 11
        val end_idx = newLine.indexOf("\",\"type\":")
        newLine = newLine.substring(start_idx, end_idx).replace("\\\"", "\"")
      }

      // 兼容非 json 格式的 nginx 日志格式
      /*if (!newLine.startsWith("{") && !newLine.endsWith("}") && !newLine.contains("\"authorization\":")) {
        newLine = log2json(newLine)
        println(newLine)
      }*/

      newLine = newLine.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      if (!newLine.contains("\"@timestamp\":")) {
        newLine = newLine.replace("\"timestamp\":", "\"@timestamp\":")
      }
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[DongqiudiNginxWWWLogModel])

      // 过滤客户端预加载请求的接口以节省存储空间
      /*if (logModel.getUri.startsWith("/v2/article/detail/")) {
        return Row(0)
      }*/

      // timestamp
      val timeMSG = LogConvertUtil.getTimeMSG(logModel.getTimestamp)
      val timestamp = timeMSG(0)
      val day = timeMSG(1)
      val hour = timeMSG(2)

      var message = LogConvertUtil.uaMap.get(logModel.getAgent)
      // platform & version
      /*var platform = "-"
      var version = "-"
      if (message == null) {
        platform = LogConvertUtil.getPlatform(logModel.getAgent)
        version = LogConvertUtil.getVersion(logModel.getAgent)
        LogConvertUtil.uaMap.put(logModel.getAgent, platform + "\t" + version)
      } else {
        val array = message.split("\t")
        platform = array(0)
        version = array(1)
      }*/

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
        logModel.getVersion,
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
        logModel.getDqdUID,
        logModel.getSign,

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
