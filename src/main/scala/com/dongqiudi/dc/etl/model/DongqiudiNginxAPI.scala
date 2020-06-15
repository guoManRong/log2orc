package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.util.{IPExt, LogConvertUtil}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * 懂球帝-API-Nginx日志
  * Created by matrix on 17/10/11.
  */
object DongqiudiNginxAPI extends Serializable with Logging {

  // 日志格式示例
  /*
  定义要生成的 DataFrame 的 schema 格式, 对应标准 Nginx 日志
  {u'@timestamp': u'2017-08-29T00:00:01+08:00',
    u'agent': u'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 NewsApp/5.6.5 NetType/4G Technology/LTE (iPhone; iOS 10.3.3; Scale/2.00) (modelIdentifier/iPhone6,2 )',
    u'authorization': u'frKzm1X7VgRiNiVB8kEhBdagoyX6rrsuKSyfbfSOM2BWC922vGC9oauyZ4ZOEURP',
    u'host': u'api.dongqiudi.com',
    u'lang': u'zh-cn',
    u'method': u'GET',
    u'protocol': u'HTTP/1.1',
    u'referer': u'',
    u'remote_addr': u'124.152.204.186',
    u'request_time': 0.172,
    u'scheme': u'http',
    u'server_addr': u'10.19.165.222',
    u'sign': u'KeIjpgKG/KIMI2/khnbt59TVfefNkNRgYAzeg/Tacu4=',
    u'size': 4856,
    u'status': 200,
    u'upstream_host': u'10.19.36.221:9000',
    u'upstream_time': u'0.171',
    u'uri': u'/team/feeds/6648',
    u'url': u'/index.php',
    u'uuid': u'@oOFn5C4JrSd2VQtbXndbYbgbt4WQbW1LD4xgqnH8tOfyJy3zSVJfS+pByfPaDsSYISpzHP84b4s=',
    u'xff': u'124.152.204.186'}*/

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
    StructField("sign", StringType),

    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class DongqiudiNginxAPILogModel {
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
    @JsonProperty("sign") @BeanProperty var sign = ""
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
      if (!newLine.startsWith("{") && !newLine.endsWith("}") && !newLine.contains("\"authorization\":")) {
        newLine = log2json(newLine)
        println(newLine)
      }

      newLine = newLine.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      if (!newLine.contains("\"@timestamp\":")) {
        newLine = newLine.replace("\"timestamp\":", "\"@timestamp\":")
      }
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[DongqiudiNginxAPILogModel])

      // 过滤客户端预加载请求的接口以节省存储空间
      if (logModel.getUri.startsWith("/v2/article/detail/")) {
        return Row(0)
      }

      // 过滤压测数据
      if (logModel.getAgent.contains("test/tsung")) {
        return Row(0)
      }

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

  def log2json(line: String): String = {
    val array = parseNginxLog(line)
    println("length: " + array.length)
    // remote_ip - scheme $time_local
    // "$request" $status $body_bytes_sent $request_time "$upstream_response_time"
    // "$http_referer" "$http_user_agent" "$http_x_forwarded_for" "uuid" "token" "lang"
    if (array.length != 15) {
      return line
    }
    var jsonLine = "{\"host\": \"api.dongqiudi.com\", \"sign\": \"-\", \"upstream_host\": \"-\", \"url\": \"-\", \"server_addr\": \"-\", "
    jsonLine += "\"remote_addr\": \"" + array(0) + "\", "
    jsonLine += "\"scheme\": \"" + array(2) + "\", "
    jsonLine += "\"@timestamp\": \"" + array(3) + "\", "

    // $request: method uri protocol
    val requestArray = array(4).split("\\s+")
    if (requestArray.length != 3) {
      return line
    }
    jsonLine += "\"method\": \"" + requestArray(0) + "\", "
    jsonLine += "\"uri\": \"" + requestArray(1) + "\", "
    jsonLine += "\"protocol\": \"" + requestArray(2) + "\", "

    jsonLine += "\"status\": " + array(5) + ", "
    jsonLine += "\"size\": " + array(6) + ", "
    jsonLine += "\"request_time\": " + array(7) + ", "
    jsonLine += "\"upstream_time\": \"" + array(8) + "\", "
    jsonLine += "\"referer\": \"" + array(9) + "\", "
    jsonLine += "\"agent\": \"" + array(10) + "\", "
    jsonLine += "\"xff\": \"" + array(11) + "\", "
    jsonLine += "\"uuid\": \"" + array(12) + "\", "
    jsonLine += "\"authorization\": \"" + array(13) + "\", "
    jsonLine += "\"lang\": \"" + array(14) + "\"}"

    jsonLine
  }

  def parseNginxLog(line: String): Array[String] = {
    var idx = line.indexOf("\"")
    if (idx >= 0) {
      val nextIdx = line.indexOf("\"", idx + 1)
      var endIdx = nextIdx + 2
      if (endIdx > line.length) {
        endIdx -= 1
      }

      val array = Array(line.substring(idx + 1, nextIdx))
      if (idx == 0) {
        if (endIdx == line.length) {
          return array
        }
        return Array.concat(array, parseNginxLog(line.substring(endIdx)))
      } else {
        val subArray = parseNginxLog(line.substring(0, idx - 1))
        if (endIdx == line.length) {
          return Array.concat(subArray, array)
        }
        return Array.concat(subArray, array, parseNginxLog(line.substring(endIdx)))
      }
    }

    idx = line.indexOf("[")
    if (idx >= 0) {
      val nextIdx = line.indexOf("]", idx + 1)
      var endIdx = nextIdx + 2
      if (endIdx > line.length) {
        endIdx -= 1
      }
      val array = Array(line.substring(idx + 1, nextIdx))
      if (idx == 0) {
        if (endIdx == line.length) {
          return array
        }
        return Array.concat(array, parseNginxLog(line.substring(endIdx)))
      } else {
        val subArray = parseNginxLog(line.substring(0, idx - 1))
        if (endIdx == line.length) {
          return Array.concat(subArray, array)
        }
        return Array.concat(subArray, array, parseNginxLog(line.substring(endIdx)))
      }
    }

    line.split("\\s+")
  }
}
