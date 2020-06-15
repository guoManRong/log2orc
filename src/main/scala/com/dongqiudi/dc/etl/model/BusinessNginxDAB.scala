package com.dongqiudi.dc.etl.model

import java.util.concurrent.ConcurrentSkipListMap

import com.dongqiudi.dc.util.{CryptoUtils, IPExt, LogConvertUtil}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * 广告-DAB服务-访问日志
  * Created by matrix on 17/11/8.
  */
object BusinessNginxDAB extends Serializable with Logging {
  // 日志格式示例
  /*
  {u'@timestamp': u'2017-11-04T00:00:01+08:00',
    u'agent': u'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/11.0 Safari/601.1',
    u'authorization': u'MqzBzJBLIAHAKBdkTWIo4WWAOIJHs8rZDypWa3k3lRjUINBoYT1Bn4e391N2aLGl',
    u'host': u'dab.dongqiudi.com',
    u'ip_area': u'1156320100',
    u'ip_area_iso': u'1156320100',
    u'lang': u'zh-cn',
    u'method': u'GET',
    u'protocol': u'HTTP/1.1',
    u'referer': u'-',
    u'remote_addr': u'58.212.46.61',
    u'request_id': u'7169caa50c008a570fc005f032932207',
    u'request_time': 0.0,
    u'scheme': u'http',
    u'server_addr': u'10.19.138.48',
    u'size': 43,
    u'status': 200,
    u'upstream_host': u'-',
    u'upstream_time': u'-',
    u'uri': u'/imp/v3?ex=QvZPBY20oidd--A--c--B--PddET2OR0RezUrXyhGcTSF6tSxMj1pgDsQTpz--B--cfV16SQM--B--5Odg__&uid=107213&cid=108842&posid=100000009&ruleid=1001&androidid=__ANDROIDID__&os=ios&net=wifi&idfa=581C1EA6-9E3C-495E-8ACD-1A95229050B9&imei=__IMEI__&mn=iPhone8,2&rid=88d23131801e921b6e07643f7ed91208&apvc=5.7.6&qt=1509724801&cp=__CP__&uuid=@hYo1mj3AMUt1FryO52FG62SwvGR657yK3mea11vvluX9Xv0aTRX4PPrliqU1nVPDo725JWqapWc=&position=24&pgid=1.2.1&platform=1&ex2=',
    u'url': u'/imp/v3',
    u'uuid': u'@hYo1mj3AMUt1FryO52FG62SwvGR657yK3mea11vvluX9Xv0aTRX4PPrliqU1nVPDo725JWqapWc=',
    u'xff': u'58.212.46.61'}
  */
  val struct = StructType(Array(
    // uri 中的 ex 参数解密后的数据
    StructField("plat_form", StringType),
    StructField("channel", StringType),
    StructField("tab", StringType),
    StructField("pos_id", StringType),
    StructField("execplan_id", StringType),

    StructField("unit_id", StringType),
    StructField("creative_id", StringType),
    StructField("cost_type", StringType),
    StructField("delivery_mode", StringType),
    StructField("agent_id", StringType),

    StructField("customer_id", StringType),
    StructField("geo", StringType),
    StructField("cost", StringType),
    StructField("content", StringType),
    StructField("audience", StringType),

    // 暴光还是点击
    StructField("deploy", StringType),

    // uri 中的其它参数
    StructField("ex2", StringType),
    StructField("pgid", StringType),
    StructField("qt", StringType),
    StructField("rid", StringType),
    StructField("position", StringType),
    StructField("cp", StringType),
    StructField("apvc", StringType),
    StructField("posid", StringType),

    // nginx 日志数据
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
    StructField("request_id", StringType),

    StructField("ip_area_iso", StringType),
    StructField("ip_area", StringType),
    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class BusinessNginxDABLogModel {
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
    @JsonProperty("ip_area") @BeanProperty var areaIP = ""
    @JsonProperty("ip_area_iso") @BeanProperty var areaIPISO = ""
    @JsonProperty("request_id") @BeanProperty var requestID = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      val newLine = line.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[BusinessNginxDABLogModel])

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

      if (!(logModel.getUrl.startsWith("/imp") || logModel.getUrl.startsWith("/click"))) {
        return Row(0)
      }

      var deploy = "imp"
      if (logModel.getUrl.startsWith("/click")) {
        deploy = "click"
      }

      val uriParams = initURIParams(logModel.getUri)

      // 解密 ex 参数
      var exArgs = getExArgs(uriParams.get("ex"), 15, 1)
      if (exArgs.length != 15) {
        exArgs = getExArgs(uriParams.get("ex"), 15, 2)
        if (exArgs.length != 15) {
          return Row(0)
        }
      }

      // 解密 ex2 参数
      var ex2 = uriParams.get("ex2")
      if (ex2 == null) {
        ex2 = "-"
      }

      // 解密异常需保留原始值
      val ex2Args = getExArgs(ex2, 1, 1)
      if (ex2Args.length == 1 && ex2Args(0).length > 0) {
        ex2 = ex2Args(0)
      }

      Row(
        exArgs(0),
        exArgs(1),
        exArgs(2),
        exArgs(3),
        exArgs(4),
        exArgs(5),
        exArgs(6),
        exArgs(7),
        exArgs(8),
        exArgs(9),
        exArgs(10),
        exArgs(11),
        exArgs(12),
        exArgs(13),
        exArgs(14),

        deploy,

        ex2,
        uriParams.get(uriKeys(1)),
        uriParams.get(uriKeys(2)),
        uriParams.get(uriKeys(3)),
        uriParams.get(uriKeys(4)),
        uriParams.get(uriKeys(5)),
        uriParams.get(uriKeys(6)),
        uriParams.get(uriKeys(7)),

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

        logModel.getRequestID,
        logModel.getAreaIPISO,
        logModel.getAreaIP,

        day,
        hour
      )
    } catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row(0)
    }
  }

  // uri 中要提取的参数
  val uriKeys = Array("ex2", "pgid", "qt", "rid", "position", "cp", "apvc", "posid")

  /**
    * 提取 uri 中的参数
    * @param uri uri
    * @return
    */
  def initURIParams(uri: String): ConcurrentSkipListMap[String, String] = {
    val uriParams = new ConcurrentSkipListMap[String, String]
    val array = uri.split("\\?")
    if (array.length == 1) {
      return uriParams
    }
    val params = array(1)
    val args = params.split("&", -1)

    for(item <- args) {
      if (item.contains("=")) {
        val idx = item.indexOf("=")
        val key = item.substring(0, idx)
        if (uriKeys.contains(key) || key.equals("ex")) {
          var value = item.substring(idx + 1)
          if (value.length == 0) value = "-"
          uriParams.put(key, value)
        }
      }
    }
    for(key <- uriKeys) {
      if (!uriParams.containsKey(key)) {
        uriParams.put(key, "-")
      }
    }
    uriParams
  }

  /**
    * 解密 ex 参值
    * @param exs 加密数据
    * @param len 解密后的数据长度
    * @return
    */
  def getExArgs(exs: String, len: Int, i: Int): Array[String] = {
    if (exs == null || exs.equals("-")) {
      val a = new Array[String](len)
      for(i <- 0 until len) {
        a(i) = "0"
      }
      return a
    }
    var ex = exs
    ex = ex.replaceAll("_", "=")
    ex = ex.replaceAll(" ", "+")

    if (i == 1) {
      ex = ex.replaceAll("--B--", "/")
      ex = ex.replaceAll("--A--", "+")
    } else {
      ex = ex.replaceAll("--A--", "+")
      ex = ex.replaceAll("--B--", "/")
    }

    /*ex = ex.replaceAll("--G--", "&")
    ex = ex.replaceAll("--F--", "#")
    ex = ex.replaceAll("--E--", "%")
    ex = ex.replaceAll("--D--", "=")
    ex = ex.replaceAll("--C--", "?")*/


    val value = CryptoUtils.decrypt(ex)
    val array = value.split(",", -1)
    if (array.length == len) {
      for(i <- 0 until array.length) {
        if (array(i).length == 0) {
          array(i) = "0"
        }
      }
      return array
    }
    Array()
  }
}
