package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.util.{IPExt, LogConvertUtil, UUIDUtils}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  *
  * Created by matrix on 17/12/11.
  */
object AllfootballNginxProxy extends Serializable with Logging {
  // 只处理正常的域名
  val filterHost = Array("stat.allfootballapp.com", "api.allfootballapp.com", "sport-data.allfootballapp.com", "feed.allfootballapp.com",
    "api.lan.aws.af", "sport-data-web.lan.aws.af", "feed.lan.aws.af", "activity.allfootballapp.com", "activity.lan.aws.af", "match.allfootballapp.com", "match.lan.aws.af")

  // 日志格式示例
  /*
  {u'afuid': u'-',
    u'agent': u'Mozilla/5.0 (Linux; Android 6.0.1; SM-G610F Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36 News/47 Android/47 AppFootball/47 SDK/23 PackageName/com.allfootball.news',
    u'authorization': u'-',
    u'host': u'api.allfootballapp.com',
    u'http_x_forwarded_for': u'-',
    u'lang': u'en-ZA',
    u'method': u'GET',
    u'product': u'allfootball',
    u'protocol': u'HTTP/1.1',
    u'referer': u'-',
    u'remote_addr': u'154.160.3.141',
    u'request_time': u'0.000',
    u'scheme': u'https',
    u'server_addr': u'172.31.2.162',
    u'size': 3566,
    u'status': u'200',
    u'subsys': u'api',
    u'timestamp': u'2017-12-04T10:59:59+08:00',
    u'trace_id': u'29126ee3da506bc62a29582afb047b37',
    u'upstream_addr': u'-',
    u'upstream_time': u'-',
    u'uri': u'/data/match/50762698/phrase',
    u'url': u'/data/match/50762698/phrase',
    u'uuid': u'354772082318151',
    u'uuidx': u'1b9fa5c991c73a035d7628caf7d2ffef'}*/

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
    StructField("trace_id", StringType),

    StructField("subsys", StringType),
    StructField("uuidx", StringType),
    StructField("afuid", StringType),
    StructField("version_name", StringType),
    StructField("package", StringType),

    StructField("request_body", StringType),

    StructField("day", StringType),
    StructField("hour", StringType),
    StructField("domain", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class AllfootballNginxProxyLogModel {
    @JsonProperty("timestamp")
    @BeanProperty var timestamp = ""
    @JsonProperty("server_addr")
    @BeanProperty var serverAddr = ""
    @JsonProperty("remote_addr")
    @BeanProperty var remoteAddr = ""
    @JsonProperty("scheme")
    @BeanProperty var scheme = ""
    @JsonProperty("host")
    @BeanProperty var host = ""
    @JsonProperty("method")
    @BeanProperty var method = ""
    @JsonProperty("uri")
    @BeanProperty var uri = ""
    @JsonProperty("url")
    @BeanProperty var url = ""
    @JsonProperty("protocol")
    @BeanProperty var protocol = ""
    @JsonProperty("status")
    @BeanProperty var status = ""
    @JsonProperty("size")
    @BeanProperty var size = ""
    @JsonProperty("request_time")
    @BeanProperty var requestTime = ""
    @JsonProperty("upstream_time")
    @BeanProperty var upstreamTime = ""
    @JsonProperty("upstream_addr")
    @BeanProperty var upstreamHost = ""
    @JsonProperty("referer")
    @BeanProperty var referer = ""
    @JsonProperty("agent")
    @BeanProperty var agent = ""
    @JsonProperty("http_x_forwarded_for")
    @BeanProperty var xff = ""
    @JsonProperty("uuid")
    @BeanProperty var uuid = ""
    @JsonProperty("authorization")
    @BeanProperty var authorization = ""
    @JsonProperty("lang")
    @BeanProperty var lang = ""
    @JsonProperty("trace_id")
    @BeanProperty var traceID = ""
    @JsonProperty("subsys")
    @BeanProperty var subsys = ""
    @JsonProperty("uuidx")
    @BeanProperty var uuidx = ""
    @JsonProperty("afuid")
    @BeanProperty var afuid = ""
    @JsonProperty("version_name")
    @BeanProperty var versionName = ""
    @JsonProperty("package")
    @BeanProperty var packageName = ""
    @JsonProperty("request_body")
    @BeanProperty var requestBody = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      var newLine = line
      if (!line.startsWith("{")) {
        newLine = "{" + line + "}"
      }
      newLine = newLine.replace("@timestamp", "timestamp")
      newLine = newLine.replace("\"xff\"", "\"http_x_forwarded_for\"")
      /*if (newLine.contains("\"message\":\"{")) {
        val start_idx = newLine.indexOf("\"message\":\"{") + 11
        val end_idx = newLine.indexOf("\",\"type\":\"in-api-nginx-new\",\"tags\":[\"_jsonparsefailure\"]}")
        newLine = newLine.substring(start_idx, end_idx).replace("\\\"", "\"")
      }*/
      newLine = newLine.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[AllfootballNginxProxyLogModel])

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
      if (platform.startsWith("i")) {
        platform = "ios"
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

      var domain = "other.allfootballapp.com"
      for (item <- filterHost) {
        if (logModel.getHost.equals(item)) {
          domain = logModel.getHost
        }
      }
      if (domain.equals("sport-data-web.lan.aws.af")) {
        domain = "sport-data.allfootballapp.com"
      } else if (domain.equals("feed.lan.aws.af")) {
        domain = "feed.allfootballapp.com"
      } else if (domain.equals("api.lan.aws.af")) {
        domain = "api.allfootballapp.com"
      } else if (domain.equals("activity.lan.aws.af")) {
        domain = "activity.allfootballapp.com"
      }

      if (logModel.getTraceID.length == 0) {
        logModel.setTraceID("-")
      }

      if (logModel.getSubsys.length == 0) {
        logModel.setSubsys("-")
      }

      if (logModel.getUuidx.length == 0) {
        logModel.setUuidx("-")
      }

      if (logModel.getAfuid.length == 0) {
        logModel.setAfuid("-")
      }

      if (logModel.getVersionName.length == 0) {
        logModel.setVersionName("-")
      }

      if (logModel.getPackageName.length == 0) {
        logModel.setPackageName("-")
      }

      if (logModel.getRequestBody.length == 0) {
        logModel.setRequestBody("-")
      }

      // 兼容客户端BUG导致部分版本上报UUID存在加密和未加密的情况
      if ((platform.equals("android") && (version.equals("84") || version.equals("86"))) ||
        platform.equals("ios") && (version.equals("2.1.0") || version.equals("2.1.1"))) {
        if (!logModel.getUuid.startsWith("@")) {
          try {
            logModel.setUuid(UUIDUtils.encrypt(logModel.getUuid))
          } catch {
            case e: Exception =>
              logError("UUID encrypt error[" + logModel.getUuid + "] msg[" + e.getMessage + "]")
          }
        }
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
        logModel.getTraceID,

        logModel.getSubsys,
        logModel.getUuidx,
        logModel.getAfuid,
        logModel.getVersionName,
        logModel.getPackageName,

        logModel.getRequestBody,

        day,
        hour,
        domain
      )
    } catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row(0)
    }
  }

  def main(args: Array[String]): Unit = {
    //var line = "\"referer\":\"-\",\"agent\":\"Mozilla/5.0 (Linux; Android 7.1.2; Redmi Note 5 Build/N2G47H; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/67.0.3396.81 Mobile Safari/537.36 NewsApp/143 Android/143 AppName/2.9.9 SDK/25 PackageName/com.allfootball.news\",\"scheme\":\"http\",\"protocol\":\"HTTP/1.1\",\"http_x_forwarded_for\":\"-\",\"host\":\"sport-data.allfootballapp.com\",\"method\":\"GET\",\"uuid\":\"@Uq99LcK46nRnmYVPYRjbGyzFLoVSEuFaM6JXbN2urpP2LKqgqOawTBQY71iBq7k4\",\"authorization\":\"-\",\"lang\":\"en-IN\",\"uri\":\"/soccer/biz/match/overview/50953009?app=af&language=en-IN\",\"url\":\"/soccer/biz/match/overview/50953009\",\"trace_id\":\"e93f8c56019697bb47134abfe567102b\",\"product\":\"allfootball\",\"subsys\":\"api\",\"upstream_addr\":\"35.178.12.142:80\",\"remote_addr\":\"-\",\"server_addr\":\"10.0.70.152\",\"request_time\":\"0.020\",\"status\":\"200\",\"timestamp\":\"2018-12-24T22:19:53+08:00\",\"size\":1813,\"upstream_time\":\"0.020\",\"uuidx\":\"-\",\"version_name\":\"2.9.9\",\"package\":\"com.allfootball.news\",\"afuid\":\"-\""
    var line = "\"referer\":\"-\",\"agent\":\"Mozilla/5.0 (Linux; Android 6.0.1; SM-J700F Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36 NewsApp/172 Android/172 AppName/3.0.9 SDK/23 PackageName/com.allfootball.news\",\"scheme\":\"http\",\"size\":0,\"protocol\":\"HTTP/1.1\",\"http_x_forwarded_for\":\"-\",\"host\":\"stat.allfootballapp.com\",\"method\":\"POST\",\"uuid\":\"@+QsYjCsh+s0U52ekZb2tJaqKpl1hgm6CUjI7zWsVarkzewop3fmetN/WI7sMckg+\",\"authorization\":\"-\",\"lang\":\"en-GB\",\"uri\":\"/analyse/push?language=en-IN\",\"url\":\"/analyse/push\",\"trace_id\":\"2d0ac5a27790aeea07e9c19eb1297ef3\",\"product\":\"allfootball\",\"subsys\":\"api\",\"upstream_addr\":\"172.31.3.96:80\",\"remote_addr\":\"106.76.27.56\",\"server_addr\":\"172.31.0.167\",\"timestamp\":\"2018-12-25T17:00:00+08:00\",\"upstream_time\":\"0.001\",\"request_time\":\"0.001\",\"status\":\"200\",\"afuid\":\"-\",\"version_name\":\"3.0.9\",\"package\":\"com.allfootball.news\",\"request_body\":\"platform=airship_gcmfcm&action=RECEIVED&msg_id=0%3A1545725711457888%2564d91d80f9fd7ecd&url=allfootball%3A%2F%2F%2Fnews%2F927945%3Ffb_ext%3Dairship_gcmFirebaseSEP874660SEP1545725703.1391&date=2018-12-25+14%3A29%3A17&timestamp=1545728357448&send_time=1545725711419&duration=2646029&\""
    parseLog(line)
  }
}
