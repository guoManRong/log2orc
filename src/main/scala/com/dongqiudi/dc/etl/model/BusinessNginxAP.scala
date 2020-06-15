package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.util.{IPExt, LogConvertUtil}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * 广告-AP服务-访问日志
  * Created by matrix on 17/10/23.
  */
object BusinessNginxAP extends Serializable with Logging {
  // 日志格式示例
  /*
  {u'@timestamp': u'2017-10-21T00:00:01+08:00',
    u'agent': u'Mozilla/5.0 (Linux; Android 5.1; MX4 Build/LMY47I) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/40.0.2214.114 Mobile Safari/537.36 News/127 Android/127 NewsApp/127 SDK/22',
    u'authorization': u'-',
    u'host': u'ap.dongqiudi.com',
    u'ip_area': u'1156371400',
    u'ip_area_iso': u'1156371400',
    u'lang': u'-',
    u'method': u'GET',
    u'protocol': u'HTTP/1.1',
    u'referer': u'-',
    u'remote_addr': u'111.14.34.23',
    u'request_id': u'29a2b65eb0c20daa7c85f83c4f7a7e2b',
    u'request_time': 0.008,
    u'scheme': u'http',
    u'server_addr': u'10.19.148.123',
    u'size': 1483,
    u'status': 200,
    u'upstream_host': u'10.19.44.199:7000',
    u'upstream_time': u'0.008',
    u'uri': u'/plat/v3?pgid=1.2.56&ct=tab_56&osv=22&apvc=5.7.4&tab=&lt=1&rs=1152*1920&pl=zh&dpr=3.0&net=wifi&bn=Meizu&os=android&trace=1&platform=1&mn=MX4&mnc=02&imei=865479022974174&gps=&channel=',
    u'url': u'/plat/v3',
    u'uuid': u'@6SnZn9/uBLVfuVpOZZWrRJEzUoRdV2TK',
    u'xff': u'111.14.34.23'}
  */
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
    StructField("request_id", StringType),

    StructField("xff", StringType),
    StructField("ip_area", StringType),
    StructField("ip_area_iso", StringType),
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

    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class BusinessNginxAPLogModel {
    @JsonProperty("@timestamp") @BeanProperty var timestamp = ""
    @JsonProperty("server_addr") @BeanProperty var serverAddr = ""
    @JsonProperty("remote_addr") @BeanProperty var remoteAddr = ""
    @JsonProperty("request_id") @BeanProperty var requestID = ""
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
    @JsonProperty("ip_area") @BeanProperty var areaIP = ""
    @JsonProperty("ip_area_iso") @BeanProperty var areaIPISO = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      val newLine = line.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[BusinessNginxAPLogModel])

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

      if (logModel.getRequestID.length == 0) {
        logModel.setRequestID("-")
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
        logModel.getRequestID,

        logModel.getXff,
        logModel.getAreaIP,
        logModel.getAreaIPISO,
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
